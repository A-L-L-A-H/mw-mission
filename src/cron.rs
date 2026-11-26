use crate::error::MissionError;
use chrono::{DateTime, Datelike, Duration, TimeZone, Timelike, Utc};

#[derive(Debug, Clone)]
pub struct CronExpression {
    minutes: [bool; 60],
    hours: [bool; 24],
    days_of_month: [bool; 32],
    months: [bool; 13],
    days_of_week: [bool; 7],
    dom_wildcard: bool,
    dow_wildcard: bool,
}

impl CronExpression {
    pub fn parse(expr: &str) -> Result<Self, MissionError> {
        let fields: Vec<&str> = expr.split_whitespace().collect();
        if fields.len() != 5 {
            return Err(MissionError::Validation(format!(
                "cron expression must have exactly 5 fields (min hour dom month dow), got {}",
                fields.len()
            )));
        }

        let (minutes_vec, _) = parse_field(fields[0], 0, 59, false)?;
        let (hours_vec, _) = parse_field(fields[1], 0, 23, false)?;
        let (dom_vec, dom_wildcard) = parse_field(fields[2], 1, 31, false)?;
        let (months_vec, _) = parse_field(fields[3], 1, 12, false)?;
        let (dow_vec, dow_wildcard) = parse_field(fields[4], 0, 7, true)?;

        let mut minutes = [false; 60];
        for minute in minutes_vec {
            minutes[minute as usize] = true;
        }

        let mut hours = [false; 24];
        for hour in hours_vec {
            hours[hour as usize] = true;
        }

        let mut days_of_month = [false; 32];
        for day in dom_vec {
            days_of_month[day as usize] = true;
        }

        let mut months = [false; 13];
        for month in months_vec {
            months[month as usize] = true;
        }

        let mut days_of_week = [false; 7];
        for weekday in dow_vec {
            let normalized = if weekday == 7 { 0 } else { weekday };
            days_of_week[normalized as usize] = true;
        }

        Ok(Self {
            minutes,
            hours,
            days_of_month,
            months,
            days_of_week,
            dom_wildcard,
            dow_wildcard,
        })
    }

    pub fn next_after(&self, after: DateTime<Utc>) -> Result<DateTime<Utc>, MissionError> {
        let truncated = Utc
            .with_ymd_and_hms(
                after.year(),
                after.month(),
                after.day(),
                after.hour(),
                after.minute(),
                0,
            )
            .single()
            .ok_or_else(|| MissionError::Internal("invalid UTC timestamp".to_string()))?;

        let mut candidate = truncated + Duration::minutes(1);
        let limit_year = after.year() + 5;

        while candidate.year() <= limit_year {
            let month = candidate.month() as usize;
            if !self.months[month] {
                let (next_year, next_month) = if candidate.month() == 12 {
                    (candidate.year() + 1, 1)
                } else {
                    (candidate.year(), candidate.month() + 1)
                };
                candidate = Utc
                    .with_ymd_and_hms(next_year, next_month, 1, 0, 0, 0)
                    .single()
                    .ok_or_else(|| MissionError::Internal("failed to advance month".to_string()))?;
                continue;
            }

            if !self.matches_day(candidate) {
                let next_day = candidate.date_naive().succ_opt().ok_or_else(|| {
                    MissionError::Internal("failed to advance calendar day".to_string())
                })?;
                candidate = Utc
                    .with_ymd_and_hms(next_day.year(), next_day.month(), next_day.day(), 0, 0, 0)
                    .single()
                    .ok_or_else(|| MissionError::Internal("failed to construct day".to_string()))?;
                continue;
            }

            if !self.hours[candidate.hour() as usize] {
                candidate = candidate + Duration::hours(1) - Duration::minutes(i64::from(candidate.minute()));
                continue;
            }

            if !self.minutes[candidate.minute() as usize] {
                candidate += Duration::minutes(1);
                continue;
            }

            return Ok(candidate);
        }

        Err(MissionError::Validation(
            "cron expression does not match any timestamp within the next 5 years".to_string(),
        ))
    }

    fn matches_day(&self, ts: DateTime<Utc>) -> bool {
        let dom_match = self.days_of_month[ts.day() as usize];
        let dow_match = self.days_of_week[ts.weekday().num_days_from_sunday() as usize];

        match (self.dom_wildcard, self.dow_wildcard) {
            (true, true) => true,
            (true, false) => dow_match,
            (false, true) => dom_match,
            (false, false) => dom_match || dow_match,
        }
    }
}

fn parse_field(
    field: &str,
    min: u32,
    max: u32,
    allow_sunday_seven: bool,
) -> Result<(Vec<u32>, bool), MissionError> {
    let trimmed = field.trim();
    if trimmed.is_empty() {
        return Err(MissionError::Validation("empty cron field".to_string()));
    }

    let is_wildcard = trimmed == "*";
    let mut values = Vec::new();

    for segment in trimmed.split(',') {
        let mut parts = segment.split('/');
        let range_part = parts
            .next()
            .ok_or_else(|| MissionError::Validation(format!("invalid cron segment: {segment}")))?;
        let step_part = parts.next();
        if parts.next().is_some() {
            return Err(MissionError::Validation(format!(
                "too many '/' tokens in cron segment: {segment}"
            )));
        }

        let step: u32 = match step_part {
            Some(raw_step) => {
                let parsed = raw_step.parse::<u32>().map_err(|_| {
                    MissionError::Validation(format!("invalid cron step: {raw_step}"))
                })?;
                if parsed == 0 {
                    return Err(MissionError::Validation(
                        "cron step must be greater than zero".to_string(),
                    ));
                }
                parsed
            }
            None => 1,
        };

        let (start, end) = if range_part == "*" {
            let effective_max = if allow_sunday_seven { 6 } else { max };
            (min, effective_max)
        } else if let Some((left, right)) = range_part.split_once('-') {
            let start_val = parse_bound(left, min, max)?;
            let end_val = parse_bound(right, min, max)?;
            if start_val > end_val {
                return Err(MissionError::Validation(format!(
                    "cron range start {start_val} exceeds end {end_val}"
                )));
            }
            (start_val, end_val)
        } else {
            let single = parse_bound(range_part, min, max)?;
            if step_part.is_some() {
                let effective_max = if allow_sunday_seven { 6 } else { max };
                (single, effective_max)
            } else {
                (single, single)
            }
        };

        let mut current = start;
        while current <= end {
            values.push(current);
            match current.checked_add(step) {
                Some(next) => current = next,
                None => break,
            }
        }
    }

    if values.is_empty() {
        return Err(MissionError::Validation(format!(
            "cron field produced no matching values: {field}"
        )));
    }

    Ok((values, is_wildcard))
}

fn parse_bound(raw: &str, min: u32, max: u32) -> Result<u32, MissionError> {
    let value = raw
        .parse::<u32>()
        .map_err(|_| MissionError::Validation(format!("invalid cron number: {raw}")))?;
    if value < min || value > max {
        return Err(MissionError::Validation(format!(
            "cron value {value} out of bounds [{min}, {max}]"
        )));
    }
    Ok(value)
}
