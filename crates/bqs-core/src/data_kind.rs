use bqs_config::DataKind;

use bq_record::DataKind as ProtoDataKind;

pub fn parse(s: &str) -> Result<DataKind, String> {
    match s.trim().to_ascii_lowercase().as_str() {
        "resnet" | "onnx" => Ok(DataKind::Resnet),
        "nnue" => Ok(DataKind::Nnue),
        other => Err(format!("未知数据类别 {other:?}（可选 resnet / nnue）")),
    }
}

pub fn to_proto(kind: DataKind) -> i32 {
    match kind {
        DataKind::Resnet => ProtoDataKind::DataResnet as i32,
        DataKind::Nnue => ProtoDataKind::DataNnue as i32,
    }
}

pub fn try_from_proto(v: i32) -> Option<DataKind> {
    match v {
        x if x == ProtoDataKind::DataResnet as i32 => Some(DataKind::Resnet),
        x if x == ProtoDataKind::DataNnue as i32 => Some(DataKind::Nnue),
        _ => None,
    }
}

pub fn display_name(kind: i64) -> &'static str {
    match kind {
        1 => "nnue",
        2 => "none",
        _ => "resnet",
    }
}

pub fn weight_format(kind: DataKind) -> &'static str {
    match kind {
        DataKind::Resnet => "onnx",
        DataKind::Nnue => "nnue",
    }
}

pub fn weight_format_for_proto(kind: i32) -> Option<&'static str> {
    try_from_proto(kind).map(weight_format)
}

pub fn proto_for_weight_format(format: &str) -> Result<i32, String> {
    match format.trim().to_ascii_lowercase().as_str() {
        "onnx" => Ok(ProtoDataKind::DataResnet as i32),
        "nnue" => Ok(ProtoDataKind::DataNnue as i32),
        other => Err(format!("未知权重格式 \"{other}\"（可选 onnx / nnue）")),
    }
}

pub fn proto_for_format(format: &str) -> i32 {
    if format == "nnue" {
        ProtoDataKind::DataNnue as i32
    } else {
        ProtoDataKind::DataResnet as i32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_accepts_legacy_onnx_alias() {
        assert_eq!(parse("resnet"), Ok(DataKind::Resnet));
        assert_eq!(parse("onnx"), Ok(DataKind::Resnet));
        assert_eq!(parse(" ONNX "), Ok(DataKind::Resnet));
        assert_eq!(parse("nnue"), Ok(DataKind::Nnue));
        assert!(parse("none").is_err());
        assert!(parse("").is_err());
    }

    #[test]
    fn proto_roundtrip() {
        assert_eq!(to_proto(DataKind::Resnet), ProtoDataKind::DataResnet as i32);
        assert_eq!(to_proto(DataKind::Nnue), ProtoDataKind::DataNnue as i32);
        assert_eq!(try_from_proto(0), Some(DataKind::Resnet));
        assert_eq!(try_from_proto(1), Some(DataKind::Nnue));
        assert_eq!(try_from_proto(2), None);
    }

    #[test]
    fn display_names_match_store_kinds() {
        assert_eq!(display_name(0), "resnet");
        assert_eq!(display_name(1), "nnue");
        assert_eq!(display_name(2), "none");
        assert_eq!(display_name(9), "resnet");
    }

    #[test]
    fn weight_format_mapping() {
        assert_eq!(weight_format(DataKind::Resnet), "onnx");
        assert_eq!(weight_format(DataKind::Nnue), "nnue");
        assert_eq!(weight_format_for_proto(0), Some("onnx"));
        assert_eq!(weight_format_for_proto(1), Some("nnue"));
        assert_eq!(weight_format_for_proto(2), None);
        assert_eq!(proto_for_weight_format("onnx"), Ok(0));
        assert_eq!(proto_for_weight_format("nnue"), Ok(1));
        assert!(proto_for_weight_format("pt").is_err());
        assert_eq!(proto_for_format("nnue"), 1);
        assert_eq!(proto_for_format("onnx"), 0);
        assert_eq!(proto_for_format("whatever"), 0);
    }
}
