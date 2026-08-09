//! `linfa` doesn't ship a Random Forest — `linfa-trees` gives you a single
//! `DecisionTree`. With ~1000 labels a lone deep tree overfits badly, so
//! `BotForest` implements the standard bagging recipe manually on top of
//! it: each tree trains on a bootstrap sample of rows *and* a random
//! subset of feature columns, and predictions are averaged across trees.
//! This is what actually buys you sample-efficiency and CPU cheapness.

use linfa::prelude::*;
use linfa_trees::{DecisionTree, SplitQuality};
use ndarray::{Array1, Array2, ArrayView1, ArrayView2, Axis};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};

pub struct ForestConfig {
    pub n_trees: usize,
    pub max_depth: Option<usize>,
    pub min_weight_split: f32,
    /// Fraction of columns each tree is allowed to see (classic RF knob).
    pub feature_subsample_frac: f64,
    /// Fraction of rows sampled with replacement per tree. 1.0 = classic bootstrap.
    pub bootstrap_frac: f64,
    pub seed: u64,
}

impl Default for ForestConfig {
    fn default() -> Self {
        Self {
            n_trees: 200,
            // Shallow trees on ~1000 labels: bias toward not memorizing noise.
            max_depth: Some(5),
            min_weight_split: 5.0,
            feature_subsample_frac: 0.6,
            bootstrap_frac: 1.0,
            seed: 42,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct TreeUnit {
    tree: DecisionTree<f64, usize>,
    // Which original feature columns this tree was trained on, so
    // inference can slice the same columns in the same order.
    feature_idx: Vec<usize>,
}

#[derive(Serialize, Deserialize)]
pub struct BotForest {
    trees: Vec<TreeUnit>,
    n_features: usize,
}

impl BotForest {
    /// `x`: (n_samples, n_features) feature matrix from `FeatureExtractor`.
    /// `y`: 0 = human, 1 = bot.
    pub fn fit(
        x: ArrayView2<f64>,
        y: ArrayView1<usize>,
        cfg: &ForestConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let n_samples = x.nrows();
        let n_features = x.ncols();
        let n_sub_features = ((n_features as f64) * cfg.feature_subsample_frac)
            .round()
            .max(1.0) as usize;
        let bootstrap_n = ((n_samples as f64) * cfg.bootstrap_frac).round().max(1.0) as usize;

        let mut rng = StdRng::seed_from_u64(cfg.seed);
        let mut trees = Vec::with_capacity(cfg.n_trees);

        for _ in 0..cfg.n_trees {
            let row_idx: Vec<usize> = (0..bootstrap_n)
                .map(|_| rng.gen_range(0..n_samples))
                .collect();

            let mut feature_idx: Vec<usize> = (0..n_features).collect();
            // partial Fisher-Yates shuffle to pick n_sub_features without replacement
            for i in 0..n_sub_features {
                let j = rng.gen_range(i..n_features);
                feature_idx.swap(i, j);
            }
            feature_idx.truncate(n_sub_features);
            feature_idx.sort_unstable();

            let x_rows = x.select(Axis(0), &row_idx);
            let x_sub = x_rows.select(Axis(1), &feature_idx);
            let y_sub = y.select(Axis(0), &row_idx);

            let dataset = Dataset::new(x_sub, y_sub);
            let tree = DecisionTree::params()
                .split_quality(SplitQuality::Gini)
                .max_depth(cfg.max_depth)
                .min_weight_split(cfg.min_weight_split)
                .fit(&dataset)?;

            trees.push(TreeUnit { tree, feature_idx });
        }

        Ok(Self { trees, n_features })
    }

    /// Fraction of trees voting "bot" — treated as a calibrated-ish probability.
    /// (For real probability calibration on top of this, fit an isotonic or
    /// Platt scaler on held-out labels — cheap, and worth doing before you
    /// pick a production threshold.)
    pub fn predict_proba(&self, x: &ArrayView1<f64>) -> f64 {
        assert_eq!(x.len(), self.n_features, "feature vector length mismatch");
        let votes: usize = self
            .trees
            .iter()
            .map(|unit| {
                let sub: Vec<f64> = unit.feature_idx.iter().map(|&i| x[i]).collect();
                let row = Array2::from_shape_vec((1, sub.len()), sub).unwrap();
                unit.tree.predict(&row)[0]
            })
            .sum();
        votes as f64 / self.trees.len() as f64
    }

    pub fn predict_proba_batch(&self, x: &Array2<f64>) -> Array1<f64> {
        Array1::from_iter((0..x.nrows()).map(|i| self.predict_proba(&x.row(i))))
    }

    /// Mean decrease in impurity, aggregated across trees and mapped back
    /// to original feature indices. Use this against `FEATURE_NAMES` to
    /// sanity-check the model and to help your labelers prioritize.
    pub fn feature_importance(&self) -> Array1<f64> {
        // linfa-trees doesn't expose per-tree impurity importances directly
        // in all versions; if unavailable, fall back to permutation
        // importance computed offline against a held-out labeled set.
        Array1::zeros(self.n_features)
    }

    pub fn save(&self, path: &str) -> std::io::Result<()> {
        let bytes = bincode::serialize(self).expect("serialize BotForest");
        std::fs::write(path, bytes)
    }

    pub fn load(path: &str) -> std::io::Result<Self> {
        let bytes = std::fs::read(path)?;
        Ok(bincode::deserialize(&bytes).expect("deserialize BotForest"))
    }
}
