use serde::{Deserialize, Serialize};

use super::RawPrestoTy;

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
    pub type_signature: Option<TypeSignature>,
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TypeSignature {
    pub raw_type: RawPrestoTy,
    pub arguments: Vec<ClientTypeSignatureParameter>,
    #[serde(skip)]
    type_arguments: (), // deprecated
    #[serde(skip)]
    literal_arguments: (), //deprecated
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NamedTypeSignature {
    pub field_name: Option<RowFieldName>,
    pub type_signature: TypeSignature,
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RowFieldName {
    pub name: String,
    #[serde(skip)]
    delimited: (), // deprecated
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(tag = "kind", content = "value")]
pub enum ClientTypeSignatureParameter {
    #[serde(rename = "TYPE_SIGNATURE")]
    TypeSignature(TypeSignature),
    #[serde(rename = "NAMED_TYPE_SIGNATURE")]
    NamedTypeSignature(NamedTypeSignature),
    #[serde(rename = "LONG")]
    LongLiteral(u64),
}

impl TypeSignature {
    pub fn new(raw_type: RawPrestoTy, arguments: Vec<ClientTypeSignatureParameter>) -> Self {
        TypeSignature {
            raw_type,
            arguments,
            type_arguments: (),
            literal_arguments: (),
        }
    }
}

impl RowFieldName {
    pub fn new(name: String) -> Self {
        RowFieldName {
            name,
            delimited: (),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sig_varchar_de() {
        let s = r#"
        {
                "rawType": "varchar",
                "typeArguments": [],
                "literalArguments": [],
                "arguments": [
                    {
                        "kind": "LONG",
                        "value": 2147483647
                    }
                ]
        }
        "#;

        let s = serde_json::from_str::<TypeSignature>(&s).unwrap();
        assert_eq!(
            s,
            TypeSignature {
                raw_type: RawPrestoTy::VarChar,
                arguments: vec![ClientTypeSignatureParameter::LongLiteral(2147483647)],
                type_arguments: (),
                literal_arguments: (),
            }
        );
    }

    #[test]
    fn test_sig_ty_de() {
        let s = r#"
        {
                "rawType": "map",
                "typeArguments": [],
                "literalArguments": [],
                "arguments": [
                    {
                        "kind": "TYPE_SIGNATURE",
                        "value": {
                            "rawType": "varchar",
                            "typeArguments": [],
                            "literalArguments": [],
                            "arguments": [
                                {
                                    "kind": "LONG",
                                    "value": 3
                                }
                            ]
                        }
                    }
                ]
            }
        "#;

        let s = serde_json::from_str::<TypeSignature>(&s).unwrap();
        assert_eq!(
            s,
            TypeSignature {
                raw_type: RawPrestoTy::Map,
                arguments: vec![ClientTypeSignatureParameter::TypeSignature(TypeSignature {
                    raw_type: RawPrestoTy::VarChar,
                    arguments: vec![ClientTypeSignatureParameter::LongLiteral(3)],
                    type_arguments: (),
                    literal_arguments: (),
                })],
                type_arguments: (),
                literal_arguments: (),
            }
        );
    }

    #[test]
    fn test_sig_named_ty_de() {
        let s = r#"
        {
                "rawType": "row",
                "typeArguments": [],
                "literalArguments": [],
                "arguments": [
                    {
                        "kind": "NAMED_TYPE_SIGNATURE",
                        "value": {
                            "fieldName": {
                                "name": "y",
                                "delimited": false
                            },
                            "typeSignature": {
                                "rawType": "double",
                                "typeArguments": [],
                                "literalArguments": [],
                                "arguments": []
                            }
                        }
                    }
                ]
            }
        "#;

        let s = serde_json::from_str::<TypeSignature>(&s).unwrap();
        assert_eq!(
            s,
            TypeSignature {
                raw_type: RawPrestoTy::Row,
                arguments: vec![ClientTypeSignatureParameter::NamedTypeSignature(
                    NamedTypeSignature {
                        field_name: Some(RowFieldName {
                            name: "y".to_string(),
                            delimited: (),
                        }),
                        type_signature: TypeSignature {
                            raw_type: RawPrestoTy::Double,
                            arguments: vec![],
                            type_arguments: (),
                            literal_arguments: (),
                        }
                    }
                )],
                type_arguments: (),
                literal_arguments: (),
            }
        );
    }
}
