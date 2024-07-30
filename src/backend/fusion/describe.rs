use std::fmt;

use arrow::datatypes::DataType;
use datafusion::{
    functions_aggregate::{count::count, expr_fn::avg, median::median, stddev::stddev, sum::sum},
    prelude::{array_length, case, cast, col, is_null, length, lit, max, min, DataFrame},
};

#[allow(unused)]
#[derive(Debug)]
pub enum DescribeMethod {
    Total,
    NullTotal,
    Mean,
    Stddev,
    Min,
    Max,
    Median,
    Percentile(u8),
}

#[allow(unused)]
pub struct DataFrameDescriber {
    original: DataFrame,
    transformed: DataFrame,
    methods: Vec<DescribeMethod>,
}

impl DataFrameDescriber {
    pub fn try_new(df: DataFrame) -> anyhow::Result<Self> {
        let fields = df.schema().fields().iter();
        let expressions = fields
            .map(|field| {
                let dt = field.data_type();
                let expr = match dt {
                    dt if dt.is_temporal() => cast(col(field.name()), DataType::Float64),
                    dt if dt.is_numeric() => col(field.name()),
                    DataType::List(_) | DataType::LargeList(_) => array_length(col(field.name())),
                    _ => length(cast(col(field.name()), DataType::Utf8)),
                };
                expr.alias(field.name())
            })
            .collect::<Vec<_>>();
        let transformed = df.clone().select(expressions)?;
        Ok(Self {
            original: df,
            transformed,
            methods: vec![
                DescribeMethod::Total,
                DescribeMethod::NullTotal,
                DescribeMethod::Mean,
                DescribeMethod::Stddev,
                DescribeMethod::Min,
                DescribeMethod::Max,
                DescribeMethod::Median,
            ],
        })
    }

    pub fn describe(&self) -> anyhow::Result<DataFrame> {
        let df = self.do_describe()?;
        let ret = self.cast_back(df)?;
        Ok(ret)
    }
    fn do_describe(&self) -> anyhow::Result<DataFrame> {
        let df: Option<DataFrame> = self.methods.iter().fold(None, |acc, method| {
            let df = self.transformed.clone();
            let stat_df = match method {
                DescribeMethod::Total => total(df).unwrap(),
                DescribeMethod::NullTotal => null_total(df).unwrap(),
                DescribeMethod::Mean => mean(df).unwrap(),
                DescribeMethod::Stddev => std_div(df).unwrap(),
                DescribeMethod::Min => minimum(df).unwrap(),
                DescribeMethod::Max => maximum(df).unwrap(),
                DescribeMethod::Median => med(df).unwrap(),
                // TODO implement percentile
                DescribeMethod::Percentile(_) => todo!(),
            };
            let stat_df = stat_df
                .with_column("stats", lit(method.to_string()))
                .unwrap();
            match acc {
                Some(acc) => Some(acc.union(stat_df).unwrap()),
                None => Some(stat_df),
            }
        });
        Ok(df.unwrap())
    }
    fn cast_back(&self, df: DataFrame) -> anyhow::Result<DataFrame> {
        let fields = self.original.schema().fields().iter();
        let mut all_expressions = vec![col("stats")];
        let mut expressions = fields
            .map(|field| {
                let dt = field.data_type();
                let expr = match dt {
                    dt if dt.is_temporal() => cast(col(field.name()), dt.clone()),
                    dt if dt.is_numeric() => col(field.name()),
                    DataType::List(_) | DataType::LargeList(_) => {
                        cast(col(field.name()), DataType::Int32)
                    }
                    _ => cast(col(field.name()), DataType::UInt32),
                };
                expr.alias(field.name())
            })
            .collect::<Vec<_>>();
        all_expressions.append(&mut expressions);
        Ok(df
            .select(all_expressions)?
            .sort(vec![col("stats").sort(true, true)])?)
    }
}

fn null_total(df: DataFrame) -> anyhow::Result<DataFrame> {
    let fields = df.schema().fields().iter();
    let ret = df.clone().aggregate(
        vec![],
        fields
            .map(|f| {
                sum(case(is_null(col(f.name())))
                    .when(lit(true), lit(1))
                    .otherwise(lit(0))
                    .unwrap())
                .alias(f.name())
            })
            .collect::<Vec<_>>(),
    )?;
    Ok(ret)
}

macro_rules! describe_method {
    ($name:ident, $method:ident) => {
        fn $name(df: DataFrame) -> anyhow::Result<DataFrame> {
            let fields = df.schema().fields().iter();
            let ret = df.clone().aggregate(
                vec![],
                fields
                    .filter(|f| f.data_type().is_numeric())
                    .map(|f| $method(col(f.name())).alias(f.name()))
                    .collect::<Vec<_>>(),
            )?;
            Ok(ret)
        }
    };
}

describe_method!(total, count);
describe_method!(mean, avg);
describe_method!(std_div, stddev);
describe_method!(minimum, min);
describe_method!(maximum, max);
describe_method!(med, median);

impl fmt::Display for DescribeMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DescribeMethod::Total => write!(f, "Total"),
            DescribeMethod::NullTotal => write!(f, "Null Total"),
            DescribeMethod::Mean => write!(f, "Mean"),
            DescribeMethod::Stddev => write!(f, "Stddev"),
            DescribeMethod::Min => write!(f, "Min"),
            DescribeMethod::Max => write!(f, "Max"),
            DescribeMethod::Median => write!(f, "Median"),
            DescribeMethod::Percentile(p) => write!(f, "Percentile {}", p),
        }
    }
}
