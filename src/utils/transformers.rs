pub struct Transformers;

impl Transformers {
    pub fn yyyymmdd_to_mmddyyyy(value: &str) -> Option<String> {
        let value = value.trim();
        if value.len() != 8 {
            return None;
        }
        if !value.chars().all(|c| c.is_ascii_digit()) {
            return None;
        }

        let year = value.get(0..4)?;
        let month = value.get(4..6)?;
        let day = value.get(6..8)?;
        Some(format!("{}/{}/{}", month, day, year))
    }

    pub fn prepend_weekday(s: &str) -> Option<String> {
        let s = s.trim();

        let normalized = if s.len() == 8 && s.chars().all(|c| c.is_ascii_digit()) {
            Self::yyyymmdd_to_mmddyyyy(s)?
        } else {
            s.to_string()
        };

        if normalized.contains('/') {
            let parts: Vec<&str> = normalized.split('/').collect();
            if parts.len() == 3 {
                let month = parts[0].parse::<i32>().ok()?;
                let day = parts[1].parse::<i32>().ok()?;
                let year = parts[2].parse::<i32>().ok()?;
                let month_names = [
                    "January",
                    "February",
                    "March",
                    "April",
                    "May",
                    "June",
                    "July",
                    "August",
                    "September",
                    "October",
                    "November",
                    "December",
                ];
                let month_name = *month_names.get((month - 1) as usize)?;
                let mut y = year;
                let m = month;
                let d = day;
                let t = [0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4];
                if m < 3 {
                    y -= 1;
                }
                let w = (y + y / 4 - y / 100 + y / 400 + t[(m - 1) as usize] + d) % 7;
                let weekday_names = [
                    "Sunday",
                    "Monday",
                    "Tuesday",
                    "Wednesday",
                    "Thursday",
                    "Friday",
                    "Saturday",
                ];
                let weekday = weekday_names[w as usize];
                return Some(format!("{}, {} {}, {}", weekday, month_name, d, year));
            }
        }

        let mut parts = normalized.splitn(2, ',');
        let before = parts.next()?;
        let after = parts.next()?.trim();
        let year = after.parse::<i32>().ok()?;
        let mut iter = before.split_whitespace();
        let month_str = iter.next()?;
        let day_str = iter.next()?;
        let day = day_str
            .chars()
            .filter(|c| c.is_ascii_digit())
            .collect::<String>()
            .parse::<i32>()
            .ok()?;
        let month = match month_str.to_lowercase().as_str() {
            "january" => 1,
            "february" => 2,
            "march" => 3,
            "april" => 4,
            "may" => 5,
            "june" => 6,
            "july" => 7,
            "august" => 8,
            "september" => 9,
            "october" => 10,
            "november" => 11,
            "december" => 12,
            _ => return None,
        };
        let mut y = year;
        let m = month;
        let d = day;
        let t = [0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4];
        if m < 3 {
            y -= 1;
        }
        let w = (y + y / 4 - y / 100 + y / 400 + t[(m - 1) as usize] + d) % 7;
        let names = [
            "Sunday",
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
        ];
        let weekday = names[w as usize];
        Some(format!("{}, {}", weekday, normalized))
    }
}
