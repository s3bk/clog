extern crate proc_macro;
use proc_macro::TokenStream;
use syn::{
    parse_macro_input, Data::Struct, DataStruct, DeriveInput, Expr, Field, Fields, Ident, Path
};
use darling::{ast, util, FromDeriveInput, FromField, FromMeta};
use quote::{format_ident, quote};

#[proc_macro_derive(SliceTrait)]
pub fn derive_slice_trait_fn(input: TokenStream) -> TokenStream {
    let original_struct = parse_macro_input!(input as DeriveInput);
    let SliceStruct {ident, data} = SliceStruct::from_derive_input(&original_struct).unwrap();

    let vis = original_struct.vis;
    let fields_ident = format_ident!("{}{}", ident, "Fields");
    let elem_ident = format_ident!("{}{}", ident, "Elem");
    let slice_ident = format_ident!("{}{}", ident, "Slice");
    let slice_mut_ident = format_ident!("{}{}", ident, "SliceMut");
    let slice_uninit_ident = format_ident!("{}{}", ident, "SliceUninit");

    let fields = data.take_struct().unwrap().fields;
    let idents: Vec<_> = fields.iter().map(|f| f.ident.as_ref().unwrap()).collect();
    let types: Vec<_> = fields.iter().map(|f| &f.ty).collect();
    let layout_ident: Vec<_> = idents.iter().map(|i| format_ident!("{i}_layout")).collect();
    let field_ident: Vec<_> = idents.iter().map(|i| format_ident!("{i}_field")).collect();
    let offset_ident: Vec<_> = idents.iter().map(|i| format_ident!("{i}_offset")).collect();


    quote! {
        #[derive(Debug)]
        #vis struct #fields_ident {
            #( #idents: (#types, usize) ),*
        }

        #[derive(Clone, Default)]
        #vis struct #elem_ident {
            #( #idents: <#types as SliceTrait>::Elem ),*
        }

        #vis struct #slice_ident<'a> {
            #( #idents: <#types as SliceTrait>::Slice::<'a> ),*
        }

        #vis struct #slice_mut_ident<'a> {
            #( #idents: <#types as SliceTrait>::SliceMut::<'a> ),*
        }

        #vis struct #slice_uninit_ident<'a> {
            #( #idents: <#types as SliceTrait>::SliceUninit::<'a> ),*
        }

        impl SliceTrait for #fields_ident {
            type Elem = #elem_ident;
            type Slice<'a> = #slice_ident::<'a>;
            type SliceMut<'a> = #slice_mut_ident::<'a>;
            type SliceUninit<'a> = #slice_uninit_ident<'a>;

            unsafe fn get(&self, raw: *mut u8, idx: usize) -> Self::Elem {
                unsafe {
                    #elem_ident {
                        #( #idents: self.#idents.0.get(raw.offset(self.#idents.1 as isize), idx) ),*
                    }
                }
            }
            unsafe fn slice<'a>(&self, raw: *mut u8, len: usize) -> Self::Slice<'a> {
                unsafe {
                    #slice_ident {
                        #( #idents: self.#idents.0.slice(raw.offset(self.#idents.1 as isize), len) ),*
                    }
                }
            }
            unsafe fn slice_mut<'a>(&self, raw: *mut u8, len: usize) -> Self::SliceMut<'a> {
                unsafe {
                    #slice_mut_ident {
                        #( #idents : self.#idents.0.slice_mut(raw.offset(self.#idents.1 as isize), len) ),*
                    }
                }
            }
            unsafe fn slice_uninit<'a>(&self, raw: *mut u8, len: usize) -> Self::SliceUninit<'a> {
                unsafe {
                    #slice_uninit_ident {
                        #( #idents : self.#idents.0.slice_uninit(raw.offset(self.#idents.1 as isize), len) ),*
                    }
                }
            }

            unsafe fn write(&self, raw: *mut u8, idx: usize, val: Self::Elem) {
                let #elem_ident { #( #idents ),* } = val;
                unsafe {
                    #( self.#idents.0.write(raw.offset(self.#idents.1 as isize), idx, #idents); )*
                }
            }

            fn layout(n: usize) -> (Layout, #fields_ident) {
                #( let (#layout_ident, #field_ident) = <#types as SliceTrait>::layout(n); )*

                let layout = Layout::from_size_align(0, 1).unwrap();

                #( let (layout, #offset_ident) = layout.extend(#layout_ident).unwrap(); )*

                let fields = #fields_ident {
                    #( #idents: (#field_ident, #offset_ident) ),*
                };

                (layout, fields)
            }
            fn copy_slice<'a, 'b>(from: Self::Slice<'a>, to: Self::SliceMut<'b>) {
                #( <#types as SliceTrait>::copy_slice(from.#idents, to.#idents); )*
            }
            fn copy_slice_uninit<'a, 'b>(from: Self::Slice<'a>, to: Self::SliceUninit<'b>) {
                #( <#types as SliceTrait>::copy_slice_uninit(from.#idents, to.#idents); )*
            }
        }
    }.into()
}

#[proc_macro_derive(Shema, attributes(clog))]
pub fn derive_shema_fn(input: TokenStream) -> TokenStream {
    let original_struct = parse_macro_input!(input as DeriveInput);
   
    let ShemaStruct { ident, data } = ShemaStruct::from_derive_input(&original_struct).unwrap();

    let vis = original_struct.vis;
    let fields = data.take_struct().unwrap().fields;
    let idents: Vec<_> = fields.iter().map(|f| f.ident.as_ref().unwrap()).collect();
    let types: Vec<_> = fields.iter().map(|f| &f.ty).collect();
    
    let builder_ident = format_ident!("{}{}", ident, "Builder");

    let data_ident = format_ident!("{}{}", ident, "Data");
    let fields_ident = format_ident!("{}{}", data_ident, "Fields");
    let elem_ident = format_ident!("{}{}", data_ident, "Elem");
    let data_slice_ident = format_ident!("{}{}", data_ident, "Slice");
    let data_slice_mut_ident = format_ident!("{}{}", data_ident, "SliceMut");

    let item_ident = format_ident!("{}{}", ident, "Item");
    
    let version_check: Vec<_> = fields.iter().map(|f| {
        let mut conds = vec![];
        if let Some(ref min) = f.min_version {
            conds.push(quote! { version >= #min });
        }
        if let Some(ref max) = f.max_version {
            conds.push(quote! { version <= #max })
        }
        match conds.as_slice() {
            [] => quote! { true },
            [c] => quote! { #c },
            [c0, c @ ..] => quote! { #c0 #( && #c )* }
        }
    }).collect();

    quote! {
        #[derive(clog_derive::SliceTrait)]
        #vis struct #data_ident {
            #( #idents: <#types as DataBuilder>::Data ),*
        }

        #[derive(Default, Clone)]
        #vis struct #builder_ident {
            soa: Owned<#fields_ident>,
            #( #idents: #types ),*
        }

        #[derive(Debug, Serialize)]
        #vis struct #item_ident<'a> {
            #( pub #idents: <#types as DataBuilder>::Item<'a> ),*
        }

        impl Shema for #builder_ident {
            type Item<'a> = #item_ident<'a>;
            type Fields = #fields_ident;
            
            fn fields(&self) -> &Owned<Self::Fields> {
                &self.soa
            }

            fn add(&mut self, item: #item_ident<'_>) {
                let compressed = #elem_ident {
                    #( #idents: self.#idents.add(item.#idents) ),*
                };
                self.soa.push(compressed);
            }
            fn get(&self, idx: usize) -> Option<#item_ident<'_>> {
                let compressed = self.soa.get(idx)?;
                Some(#item_ident {
                    #( #idents: self.#idents.get(compressed.#idents)? ),*
                })
            }
            fn decompress(&self, c: <#fields_ident as SliceTrait>::Elem) -> #item_ident {
                #item_ident {
                    #( #idents: self.#idents.get(c.#idents).expect(stringify!(#idents)) ),*
                }
            }

            #[cfg(feature="encode")]
            fn write(&self, f: &FileCompressor, mut writer: BytesMut, opt: &Options, version: u32) -> Result<BytesMut, Error> {
                let mut scratch = Vec::with_capacity(8 * self.soa.len() + 100);
                let #data_slice_ident { #( #idents ),* } = self.soa.slice();
                #(
                    println!("FIELD {}", stringify!(#idents));
                    if #version_check {
                        let (field_size, scratch2) = self.#idents.write(f, #idents, scratch, opt)?;
                        scratch = scratch2;
                        
                        println!("    header at {}", writer.len());
                        writer = clog::shema::encode(field_size, writer)?;
                        println!("    data at {}", writer.len());
                        writer.extend_from_slice(&scratch);
                        scratch.clear();
                    } else {
                        println!("    skipped");
                    }
                )*
                Ok(writer)
            }

            fn read<'a>(f: &FileDecompressor, data: Input<'a>, len: usize, version: u32) -> Result<(Self, Input<'a>), Error> {
                let mut soa = Owned::<#fields_ident>::default();
                soa.reserve(len as usize);
                soa.extend(std::iter::repeat(Default::default()).take(len as usize));

                let #data_slice_mut_ident {
                    #( #idents ),*
                } = soa.slice_mut();
                #(
                    println!("FIELD {}", stringify!(#idents));
                    let (#idents, data) = if #version_check {
                        println!("    header at {}", data.pos());
                        let (field_size, data) = clog::shema::decode(data)?;
                        
                        println!("    data at {}", data.pos());
                        <#types as DataBuilder>::read(f, #idents, data, field_size)?
                    } else {
                        println!("    skipped");
                        (Default::default(), data)
                    };
                )*
                
                Ok((#builder_ident {
                    soa,
                    #( #idents ),*
                }, data))
            }
            fn reserve(&mut self, additional: usize) {
                self.soa.reserve(additional);
            }
        }
    }.into()
}

#[derive(Debug, FromField)]
struct SliceField {
    ident: Option<syn::Ident>,
    ty: syn::Type,
}


#[derive(Debug, FromDeriveInput)]
struct SliceStruct {
    ident: Ident,
    data: ast::Data<util::Ignored, SliceField>,
}

#[derive(Debug, FromField)]
#[darling(attributes(clog))]
struct Entry {
    ident: Option<syn::Ident>,
    vis: syn::Visibility,
    ty: syn::Type,

    min_version: Option<Expr>,
    max_version: Option<Expr>,
}


#[derive(Debug, FromDeriveInput)]
struct ShemaStruct {
    ident: Ident,
    data: ast::Data<util::Ignored, Entry>,

}
