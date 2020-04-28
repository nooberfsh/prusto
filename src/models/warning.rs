use serde::Deserialize;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Warning {
    warning_code: Code,
    message: String,
}

#[derive(Deserialize, Debug)]
pub struct Code {
    code: i32,
    name: String,
}
