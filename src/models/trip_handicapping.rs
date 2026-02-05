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