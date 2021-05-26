use std::fmt;
use std::borrow::Cow;

use serde::{Deserialize, Serialize, Serializer, Deserializer};
use serde::ser::SerializeStruct;
use serde::de::{self, Visitor, MapAccess};

use super::RawPrestoTy;

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
    pub type_signature: Option<TypeSignature>,
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TypeSignature {
    pub raw_type: RawPrestoTy,
    pub arguments: Vec<ClientTypeSignatureParameter>,
    #[serde(skip)]
    type_arguments: (), // deprecated
    #[serde(skip)]
    literal_arguments: (), //deprecated
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NamedTypeSignature {
    pub field_name: Option<RowFieldName>,
    pub type_signature: TypeSignature,
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RowFieldName {
    pub name: String,
    #[serde(skip)]
    delimited: (), // deprecated
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientTypeSignatureParameter {
    TypeSignature(TypeSignature),
    NamedTypeSignature(NamedTypeSignature),
    LongLiteral(u64),
}

impl Serialize for ClientTypeSignatureParameter {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use ClientTypeSignatureParameter::*;
        let mut state = serializer.serialize_struct("ClientTypeSignatureParameter", 2)?;
        match self {
            TypeSignature(s) =>  {
                state.serialize_field("kind", "TYPE")?;
                state.serialize_field("value", s)?;
            },
            NamedTypeSignature(s) =>  {
                state.serialize_field("kind", "NAMED_TYPE")?;
                state.serialize_field("value", s)?;
            },
            LongLiteral(s) =>  {
                state.serialize_field("kind", "LONG")?;
                state.serialize_field("value", s)?;
            },
        };
        state.end()
    }
}

impl<'de> Deserialize<'de> for ClientTypeSignatureParameter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "lowercase")]
        enum Field {Kind, Value}

        struct ParamVisitor;

        impl<'de> Visitor<'de> for ParamVisitor {
            type Value = ClientTypeSignatureParameter;
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct ClientTypeSignatureParameter")
            }

            fn visit_map<V>(self, mut map: V) -> Result<ClientTypeSignatureParameter, V::Error>
            where
                V: MapAccess<'de>,
            {
                let kind = if let Some(Field::Kind) = map.next_key()? {
                    // this is can't be `&str`
                    // https://github.com/serde-rs/serde/issues/1009
                    // https://github.com/serde-rs/serde/issues/1413#issuecomment-494892266
                    map.next_value::<Cow<'_, str>>()?
                } else {
                    return Err(de::Error::missing_field("kind"));
                };
                if let Some(Field::Value) = map.next_key()?{
                  match kind.as_ref()  {
                      "TYPE" | "TYPE_SIGNATURE" => {
                          let v = map.next_value()?;
                          Ok(ClientTypeSignatureParameter::TypeSignature(v))
                      }
                      "NAMED_TYPE" | "NAMED_TYPE_SIGNATURE" => {
                          let v = map.next_value()?;
                          Ok(ClientTypeSignatureParameter::NamedTypeSignature(v))
                      }
                      "LONG" | "LONG_LITERAL" => {
                          let v = map.next_value()?;
                          Ok(ClientTypeSignatureParameter::LongLiteral(v))
                      }
                      k => Err(de::Error::custom(format!("unknown kind: {}", k)))
                  }
                } else {
                    Err(de::Error::missing_field("value"))
                }
            }
        }

        const FIELDS: &'static [&'static str] = &["kind", "value"];
        deserializer.deserialize_struct("ClientTypeSignatureParameter", FIELDS, ParamVisitor)
    }
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

    #[test]
    fn test_sig_param() {
        let s = r#"{"kind":"LONG","value":10}"#;
        let res = serde_json::from_str::<ClientTypeSignatureParameter>(s).unwrap();
        assert_eq!(res, ClientTypeSignatureParameter::LongLiteral(10));

        let json = serde_json::to_value(res.clone()).unwrap();
        let res2: ClientTypeSignatureParameter  = serde_json::from_value(json).unwrap();
        assert_eq!(res, res2)
    }
}
