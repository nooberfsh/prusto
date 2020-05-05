use syn::*;
use quote::*;
use syn::spanned::Spanned;
use proc_macro2::*;

#[proc_macro_derive(Presto)]
pub fn derive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let data = parse_macro_input!(input as ItemStruct);

    match derive_impl(data) {
        Ok(d) => d.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

fn derive_impl(data: ItemStruct) -> Result<TokenStream> {
    let name = &data.ident;
    let fields: Vec<Field> = match data.fields {
        Fields::Named(d) => {
            d.named.into_iter().collect()
        },
        Fields::Unnamed(d) => {
            return Err(Error::new(d.span(), "field must be named"))
        },
        Fields::Unit => return Err(Error::new(data.span(), "field can not be unit")),
    };

    let keys = fields.iter().map(|f|f.ident.as_ref().unwrap());
    let keys_lit = keys.clone()
        .map(|ident| LitStr::new(&format!("{}", ident), ident.span()));
    let types = fields.iter().map(|f|&f.ty);
    let types2 = types.clone();

    let (impl_generics, ty_generics, where_clause) = data.generics.split_for_impl();
    let gen = quote! {
        impl #impl_generics ::presto::types::Presto for #name #ty_generics #where_clause {
            type ValueType<'a> = ( #(&'a #types),* );

            fn value(&self) -> Self::ValueType<'_>  {
                ( #(& self.#keys),* )
            }

            fn ty() -> ::presto::types::PrestoTy {
                let types = vec![ #((#keys_lit.into(), <#types2 as ::presto::types::Presto>::ty())),* ];
                ::presto::types::PrestoTy::Row(types)
            }
        }
    };

    Ok(gen)
}
