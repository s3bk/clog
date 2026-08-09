use crate::features::FeatureExtractor;
use crate::model::BotForest;
use crate::types::ReqItem;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Human,
    Bot,
    /// Falls between thresholds — route to a JS challenge / CAPTCHA /
    /// manual-review queue rather than a hard allow/block. These are also
    /// your best active-learning candidates: label these first.
    Uncertain,
}

pub struct BotClassifier {
    extractor: FeatureExtractor,
    forest: BotForest,
    threshold_bot: f64,
    threshold_human: f64,
}

impl BotClassifier {
    pub fn new(extractor: FeatureExtractor, forest: BotForest) -> Self {
        Self {
            extractor,
            forest,
            // Start conservative; tune both thresholds against your held-out
            // labeled slice (precision/recall per traffic segment, not just
            // in aggregate) before shipping as an enforcement gate.
            threshold_bot: 0.75,
            threshold_human: 0.25,
        }
    }

    pub fn load(model_path: &str, extractor: FeatureExtractor) -> std::io::Result<Self> {
        let forest = BotForest::load(model_path)?;
        Ok(Self::new(extractor, forest))
    }

    pub fn with_thresholds(mut self, human_max: f64, bot_min: f64) -> Self {
        self.threshold_human = human_max;
        self.threshold_bot = bot_min;
        self
    }

    /// Probability this request is a bot, in [0, 1].
    /// NOTE: this call mutates the extractor's per-IP behavioral state
    /// (request-rate window etc.) — call it exactly once per request, in
    /// request order, not speculatively/twice.
    pub fn score(&self, item: &ReqItem) -> f64 {
        let feats = self.extractor.extract(item);
        self.forest.predict_proba(&feats.view())
    }

    pub fn classify(&self, item: &ReqItem) -> (f64, Verdict) {
        let p = self.score(item);
        let verdict = if p >= self.threshold_bot {
            Verdict::Human
        } else if p <= self.threshold_human {
            Verdict::Bot
        } else {
            Verdict::Uncertain
        };
        (p, verdict)
    }
}
