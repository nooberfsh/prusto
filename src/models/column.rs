use serde::Deserialize;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
    pub type_signature: Option<TypeSignature>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TypeSignature {
    pub raw_type: String,
    #[serde(skip)] // TODO: remove this after ClientTypeSignatureParameter impl Deserialize
    pub arguments: Vec<ClientTypeSignatureParameter>,
    #[serde(skip)]
    type_arguments: (),
    #[serde(skip)]
    literal_arguments: (),
}

// TODO: impl Deserialize
#[derive(Debug)]
pub enum ClientTypeSignatureParameter {
    Type(String),
    NamedType(String),
    Long(u64),
}
