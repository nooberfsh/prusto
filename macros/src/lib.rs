use proc_macro2::*;
use quote::*;
use syn::spanned::Spanned;
use syn::*;

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
    let vis = &data.vis;
    let seed_name = format_ident!("__{}Seed", name);
    let fields: Vec<Field> = match data.fields {
        Fields::Named(d) => d.named.into_iter().collect(),
        Fields::Unnamed(d) => return Err(Error::new(d.span(), "field must be named")),
        Fields::Unit => return Err(Error::new(data.span(), "field can not be unit")),
    };

    let keys = fields.iter().map(|f| f.ident.as_ref().unwrap());
    let keys_lit = keys
        .clone()
        .map(|ident| LitStr::new(&format!("{}", ident), ident.span()));
    let types = fields.iter().map(|f| &f.ty);
    let types2 = types.clone();

    let (impl_generics, ty_generics, where_clause) = data.generics.split_for_impl();

    let mut seed_generics = data.generics.clone();
    seed_generics.params.push(parse_quote!('_a));
    let (_, seed_ty_generics, seed_where_clause) = seed_generics.split_for_impl();

    let mut seed_de_generics = seed_generics.clone();
    seed_de_generics.params.push(parse_quote!('_de));
    let (seed_de_impl_generics, _, _) = seed_de_generics.split_for_impl();

    let gen = quote! {

        impl #impl_generics ::presto::types::Presto for #name #ty_generics #where_clause {
            type ValueType<'_a> = ( #(<#types as ::presto::types::Presto>::ValueType<'_a>),* );
            type Seed<'_a, '_de> = #seed_name #seed_ty_generics;

            fn value(&self) -> Self::ValueType<'_>  {
                ( #(self.#keys.value()),* )
            }

            fn ty() -> ::presto::types::PrestoTy {
                let types = vec![ #((#keys_lit.into(), <#types2 as ::presto::types::Presto>::ty())),* ];
                ::presto::types::PrestoTy::Row(types)
            }

            fn seed<'_a, '_de>(ty: &'_a ::presto::types::PrestoTy) -> ::std::result::Result<Self::Seed<'_a, '_de>, ::presto::types::Error> {
                if let ::presto::types::PrestoTy::Row(tyes)  = ty {
                    //TODO: add more checks
                    Ok(#seed_name(&*tyes, ::std::marker::PhantomData))
                } else {
                    Err(::presto::types::Error::InvalidPrestoType)
                }
            }
        }

        #vis struct #seed_name #seed_ty_generics (&'_a [(::std::string::String,::presto::types::PrestoTy)], ::std::marker::PhantomData<#name #ty_generics>) #seed_where_clause ;

        impl #seed_de_impl_generics ::serde::de::DeserializeSeed<'_de> for #seed_name #seed_ty_generics #seed_where_clause {
            type Value = #name #ty_generics;

            fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: ::serde::de::Deserializer<'_de>,
            {
                deserializer.deserialize_seq(self)
            }
        }

        impl #seed_de_impl_generics ::serde::de::Visitor<'_de> for #seed_name #seed_ty_generics #seed_where_clause {
            type Value = #name #ty_generics;

            fn expecting(&self, formatter: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
                formatter.write_str(" todo")
            }
        }
    };

    Ok(gen)
}
