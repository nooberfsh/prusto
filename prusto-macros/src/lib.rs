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

    let tuplety = Ident::new(&format!("Tuple{}", fields.len()), Span::call_site());

    let keys = fields.iter().map(|f| f.ident.as_ref().unwrap());
    let keys_lit = keys
        .clone()
        .map(|ident| LitStr::new(&format!("{}", ident), ident.span()));
    let types = fields.iter().map(|f| &f.ty);
    let types1 = types.clone();

    let keys2 = keys.clone();
    let types2 = types.clone();
    let types3 = types.clone();

    let (impl_generics, ty_generics, where_clause) = data.generics.split_for_impl();

    let mut seed_generics = data.generics.clone();
    seed_generics.params.push(parse_quote!('_a));
    let (seed_impl_generics, seed_ty_generics, _) = seed_generics.split_for_impl();

    let mut seed_de_generics = seed_generics.clone();
    seed_de_generics.params.push(parse_quote!('_de));
    let (seed_de_impl_generics, _, _) = seed_de_generics.split_for_impl();

    let impl_trait_block = quote! {

        impl #impl_generics ::prusto::types::Presto for #name #ty_generics #where_clause {
            type ValueType<'_a> where #(#types: '_a ,)* = ::prusto::tuples::#tuplety< #(<#types1 as ::prusto::types::Presto>::ValueType<'_a>, )* >;
            type Seed<'_a, '_de> = #seed_name #seed_ty_generics;

            fn value(&self) -> Self::ValueType<'_>  {
                ::prusto::tuples::#tuplety( #(self.#keys.value(), )* )
            }

            fn ty() -> ::prusto::types::PrestoTy {
                let types = vec![ #((#keys_lit.into(), <#types2 as ::prusto::types::Presto>::ty())),* ];
                ::prusto::types::PrestoTy::Row(types)
            }

            fn seed<'_a, '_de>(ctx: &'_a ::prusto::types::Context<'_a>) -> Self::Seed<'_a, '_de> {
                if let ::prusto::types::PrestoTy::Row(types)  = ctx.ty() {
                    let row_map = ctx.row_map().expect("invalid context");
                    if row_map.len() != types.len() {
                        panic!("invalid context");
                    }
                    #seed_name {
                        ctx,
                        types,
                        row_map,
                        _marker: ::std::marker::PhantomData,
                    }
                } else {
                    panic!("invalid context")
                }
            }

            fn empty() -> Self {
                Self {
                    #( #keys2:  <#types3 as ::prusto::types::Presto>::empty(),)*
                }
            }
        }

        #vis struct #seed_name #seed_impl_generics #where_clause {
            ctx: &'_a ::prusto::types::Context<'_a>,
            types: &'_a [(::std::string::String,::prusto::types::PrestoTy)],
            row_map: &'_a [usize],
            _marker: ::std::marker::PhantomData<#name #ty_generics>,
        }

        impl #seed_de_impl_generics ::serde::de::DeserializeSeed<'_de> for #seed_name #seed_ty_generics #where_clause {
            type Value = #name #ty_generics;

            fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: ::serde::de::Deserializer<'_de>,
            {
                deserializer.deserialize_seq(self)
            }
        }

        impl #seed_de_impl_generics ::serde::de::Visitor<'_de> for #seed_name #seed_ty_generics #where_clause {
            type Value = #name #ty_generics;

            fn expecting(&self, formatter: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
                formatter.write_str("sequence of same or different presto type")
            }

            fn visit_seq<_A: ::serde::de::SeqAccess<'_de>>(self, mut seq: _A) -> Result<Self::Value, _A::Error> {
                let mut ret = Self::Value::empty();

                for (idx, ty) in self.row_map.iter().zip(self.types.iter().map(|r| &r.1)) {
                    let ctx = self.ctx.with_ty(ty);
                    ret.__access_seq(*idx, &mut seq, &ctx)?;
                }

                if let Ok(None) = seq.next_element::<String>() {
                    Ok(ret)
                } else {
                    Err(<_A::Error as ::serde::de::Error>::custom("access seq failed, there are some extra data"))
                }
            }
        }
    };

    let impl_block = access_seq(&fields, name, &data.generics)?;

    let ret = quote! {
        #impl_trait_block
        #impl_block
    };

    Ok(ret)
}

fn access_seq(fields: &[Field], name: &Ident, generics: &Generics) -> Result<TokenStream> {
    let indices = (0..fields.len()).map(|i| LitInt::new(&format!("{}", i), fields[i].span()));
    let keys = fields.iter().map(|f| f.ident.as_ref().unwrap());
    let types = fields.iter().map(|f| &f.ty);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let access_seq = quote! {
        impl #impl_generics #name #ty_generics #where_clause {
            fn __access_seq<'_a, '_de, _A: ::serde::de::SeqAccess<'_de>>(&mut self, idx: usize, seq: &mut _A, ctx: &'_a ::prusto::types::Context<'_a>)
                -> ::std::result::Result<(), _A::Error> {
                match idx {
                    #(
                        #indices => {
                            let seed = <#types as ::prusto::types::Presto>::seed(ctx);
                            let data = seq.next_element_seed(seed)?;
                            if let Some(data) = data {
                                self.#keys = data;
                                Ok(())
                            } else {
                                Err(<_A::Error as ::serde::de::Error>::custom("access seq failed, no more data"))
                            }
                        },
                    )*
                    _ => unreachable!(),
                }
            }
        }
    };

    Ok(access_seq)
}
