#[derive(Debug, Clone)]
pub struct TripScore {
    pub raw: f64,
    pub adj: f64,
    pub headline: String,
    pub why: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct BetBackPick {
    pub pp_index: usize,
    pub score: TripScore,
    pub surface: String,
    pub dist_f: f64,
    pub date: String,
    pub track: String,
    pub adj_points: f64,
}

#[derive(Debug, Clone)]
pub struct TripResult {
    pub score: i32,
    pub days_back_1st: i64,
    pub comment_1st: String,
    pub days_back_2nd: i64,
    pub comment_2nd: String,
    pub days_back_3rd: i64,
    pub comment_3rd: String,
}
