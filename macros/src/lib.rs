//! Static namespace and event exports, collected once at application startup.
use proc_macro::TokenStream;
use proc_macro2::TokenStream as Tokens;
use quote::{format_ident, quote};
use std::collections::BTreeSet;
use syn::ext::IdentExt;
use syn::{
    Expr, FnArg, Ident, ItemFn, LitStr, Path, Token, Type,
    parse::{Parse, ParseStream},
    parse_macro_input,
};

fn server() -> Tokens {
    match proc_macro_crate::crate_name("brz-socket-io") {
        Ok(proc_macro_crate::FoundCrate::Name(name)) => {
            let name = format_ident!("{name}");
            quote!(::#name)
        }
        _ => quote!(::brz_socket_io),
    }
}
fn module(mut path: Path) -> Path {
    let segment = path.segments.last_mut().expect("group path");
    segment.ident = format_ident!("__socket_registry_{}", segment.ident.unraw());
    if path.leading_colon.is_none() && path.segments.len() == 1 {
        syn::parse_quote!(crate::#path)
    } else {
        path
    }
}
struct Registry {
    group: Ident,
    dependencies: Vec<(Ident, Type)>,
    adapter: Type,
}
impl Parse for Registry {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let mut group = None;
        let mut adapter = None;
        let mut dependencies = None;
        while !input.is_empty() {
            let key: Ident = input.parse()?;
            match key.to_string().as_str() {
                "dependencies" if dependencies.is_none() => {
                    let content;
                    syn::parenthesized!(content in input);
                    let mut fields = Vec::new();
                    let mut names = BTreeSet::new();
                    while !content.is_empty() {
                        let name: Ident = content.parse()?;
                        if !names.insert(name.to_string()) {
                            return Err(syn::Error::new_spanned(name, "duplicate dependency"));
                        }
                        content.parse::<Token![:]>()?;
                        fields.push((name, content.parse()?));
                        if !content.is_empty() {
                            content.parse::<Token![,]>()?;
                        }
                    }
                    dependencies = Some(fields);
                }
                "group" if group.is_none() => {
                    input.parse::<Token![=]>()?;
                    group = Some(input.parse()?);
                }
                "adapter" if adapter.is_none() => {
                    input.parse::<Token![=]>()?;
                    adapter = Some(input.parse()?);
                }
                _ => {
                    return Err(syn::Error::new_spanned(
                        key,
                        "expected group, adapter, or dependencies(...), each once",
                    ));
                }
            }
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }
        let server = server();
        Ok(Self {
            group: group.unwrap_or_else(|| syn::parse_quote!(socket_apis)),
            dependencies: dependencies.unwrap_or_default(),
            adapter: adapter.unwrap_or_else(|| syn::parse_quote!(#server::adapter::LocalAdapter)),
        })
    }
}
/// Declare a named collection and its shared dependencies. Use `adapter =
/// RedisAdapter` for Redis; handlers keep native typed extractors.
#[proc_macro]
pub fn registry(input: TokenStream) -> TokenStream {
    let Registry {
        group,
        dependencies,
        adapter,
    } = parse_macro_input!(input as Registry);
    let server = server();
    let module = module(syn::parse_quote!(#group));
    let module = module.segments.last().unwrap().ident.clone();
    let state = format_ident!("__SocketDependencies_{group}");
    let adapter_alias = format_ident!("__SocketAdapter_{group}");
    let fields = dependencies.iter().map(|(name, ty)| quote!(pub #name: #ty));
    quote! {
        #[doc(hidden)] #[allow(non_camel_case_types)]
        pub(crate) struct #state { #(#fields),* }
        #[doc(hidden)] #[allow(non_camel_case_types)]
        pub(crate) type #adapter_alias = #adapter;
        #[doc(hidden)] pub(crate) mod #module {
            pub type Dependencies = super::#state;
            pub type Adapter = super::#adapter_alias;
            pub struct Namespace(pub #server::__private::NamespaceRegistration<Dependencies, Adapter>);
            pub struct Event(pub #server::__private::EventRegistration<Dependencies, Adapter>);
            pub struct Disconnect(pub #server::__private::DisconnectRegistration<Dependencies>);
            #server::__private::inventory::collect!(Namespace);
            #server::__private::inventory::collect!(Event);
            #server::__private::inventory::collect!(Disconnect);
            pub async fn collect(io: &#server::SocketIo<Adapter>, dependencies: Dependencies) -> Result<(), #server::RegistryError> {
                let namespaces: Vec<_> = #server::__private::inventory::iter::<Namespace>.into_iter().map(|e| &e.0).collect();
                let events: Vec<_> = #server::__private::inventory::iter::<Event>.into_iter().map(|e| &e.0).collect();
                let disconnects: Vec<_> = #server::__private::inventory::iter::<Disconnect>.into_iter().map(|e| &e.0).collect();
                #server::__private::collect(io, dependencies, &namespaces, &events, &disconnects).await
            }
        }
    }.into()
}
struct Collect {
    io: Expr,
    dependencies: Vec<(Ident, Expr)>,
    group: Path,
}
impl Parse for Collect {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let io = input.parse()?;
        let mut dependencies = Vec::new();
        let mut names = BTreeSet::new();
        while input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
            if input.is_empty() || input.peek(Token![;]) {
                break;
            }
            let name: Ident = input.parse()?;
            if !names.insert(name.to_string()) {
                return Err(syn::Error::new_spanned(name, "duplicate dependency"));
            }
            let value = if input.peek(Token![=]) {
                input.parse::<Token![=]>()?;
                input.parse()?
            } else {
                syn::parse_quote!(#name)
            };
            dependencies.push((name, value));
        }
        let mut group = syn::parse_quote!(socket_apis);
        if input.peek(Token![;]) {
            input.parse::<Token![;]>()?;
            let key: Ident = input.parse()?;
            if key != "group" {
                return Err(syn::Error::new_spanned(key, "expected group = path"));
            }
            input.parse::<Token![=]>()?;
            group = input.parse()?;
        }
        Ok(Self {
            io,
            dependencies,
            group,
        })
    }
}
/// Register exported namespaces/events against an existing SocketIo handle.
#[proc_macro]
pub fn handlers(input: TokenStream) -> TokenStream {
    let Collect {
        io,
        dependencies,
        group,
    } = parse_macro_input!(input as Collect);
    let group = module(group);
    let fields = dependencies
        .iter()
        .map(|(name, value)| quote!(#name: #value));
    quote!(#group::collect(#io, #group::Dependencies { #(#fields),* })).into()
}
struct Export {
    name: LitStr,
    namespace: Option<LitStr>,
    group: Path,
    middleware: Option<Path>,
    fallback: bool,
}
impl Parse for Export {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let fallback = input.peek(Ident);
        let name = if fallback {
            let keyword: Ident = input.parse()?;
            if keyword != "fallback" {
                return Err(syn::Error::new_spanned(
                    keyword,
                    "expected event name or fallback",
                ));
            }
            LitStr::new("<fallback>", keyword.span())
        } else {
            input.parse()?
        };
        let mut namespace = None;
        let mut group = None;
        let mut middleware = None;
        while !input.is_empty() {
            input.parse::<Token![,]>()?;
            if input.is_empty() {
                break;
            }
            let key: Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            match key.to_string().as_str() {
                "namespace" if namespace.is_none() => namespace = Some(input.parse()?),
                "group" if group.is_none() => group = Some(input.parse()?),
                "middleware" if middleware.is_none() => middleware = Some(input.parse()?),
                _ => {
                    return Err(syn::Error::new_spanned(
                        key,
                        "unexpected or duplicate export option",
                    ));
                }
            }
        }
        Ok(Self {
            name,
            namespace,
            group: group.unwrap_or_else(|| syn::parse_quote!(socket_apis)),
            middleware,
            fallback,
        })
    }
}
struct MiddlewareArgs {
    group: Path,
}
impl Parse for MiddlewareArgs {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let group = if input.is_empty() {
            syn::parse_quote!(socket_apis)
        } else {
            let key: Ident = input.parse()?;
            if key != "group" {
                return Err(syn::Error::new_spanned(key, "expected group = path"));
            }
            input.parse::<Token![=]>()?;
            let group = input.parse()?;
            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
            group
        };
        Ok(Self { group })
    }
}
struct DisconnectArgs {
    namespace: LitStr,
    group: Path,
}
impl Parse for DisconnectArgs {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let key: Ident = input.parse()?;
        if key != "namespace" {
            return Err(syn::Error::new_spanned(
                key,
                "expected namespace = \"/...\"",
            ));
        }
        input.parse::<Token![=]>()?;
        let namespace = input.parse()?;
        let group = if input.is_empty() {
            syn::parse_quote!(socket_apis)
        } else {
            input.parse::<Token![,]>()?;
            if input.is_empty() {
                syn::parse_quote!(socket_apis)
            } else {
                input.parse::<MiddlewareArgs>()?.group
            }
        };
        Ok(Self { namespace, group })
    }
}
/// Authenticate a namespace and return its typed business identity.
/// `#[payload]` is the client's connect auth object; named `#[inject(...)]`
/// dependencies and `#[context]` handshake metadata are also available.
#[proc_macro_attribute]
pub fn middleware(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as MiddlewareArgs);
    let mut function = parse_macro_input!(item as ItemFn);
    match expand_middleware(args, &mut function) {
        Ok(t) => t.into(),
        Err(e) => e.into_compile_error().into(),
    }
}
/// Export a namespace. Authentication and the business connection handler both
/// finish before the native namespace CONNECT acknowledgement is sent.
#[proc_macro_attribute]
pub fn namespace(args: TokenStream, item: TokenStream) -> TokenStream {
    expand(args, item, false)
}
/// Export a named event, or `event(fallback, namespace = "/...")` for events
/// without a named handler. Return a business `Result<T, E>` or `Result<Reply<T>, E>`.
#[proc_macro_attribute]
pub fn event(args: TokenStream, item: TokenStream) -> TokenStream {
    expand(args, item, true)
}
/// Export an optional business teardown hook. Saved `#[auth]`, `#[session]`
/// and `#[context]` remain readable; framework cleanup runs regardless of errors.
#[proc_macro_attribute]
pub fn disconnect(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as DisconnectArgs);
    let mut function = parse_macro_input!(item as ItemFn);
    match expand_disconnect(args, &mut function) {
        Ok(t) => t.into(),
        Err(e) => e.into_compile_error().into(),
    }
}
fn expand(args: TokenStream, item: TokenStream, event: bool) -> TokenStream {
    let args = parse_macro_input!(args as Export);
    let mut function = parse_macro_input!(item as ItemFn);
    match export(args, &mut function, event) {
        Ok(t) => t.into(),
        Err(e) => e.into_compile_error().into(),
    }
}
fn check_function(function: &ItemFn) -> syn::Result<()> {
    if function.sig.asyncness.is_none()
        || !function.sig.generics.params.is_empty()
        || function.sig.receiver().is_some()
    {
        return Err(syn::Error::new_spanned(
            &function.sig,
            "export an async, non-generic free function",
        ));
    }
    Ok(())
}
fn namespace_name(name: &LitStr) -> syn::Result<()> {
    let value = name.value();
    if !value.starts_with('/')
        || value
            .chars()
            .any(|c| c.is_whitespace() || c.is_control() || matches!(c, '?' | '#'))
    {
        return Err(syn::Error::new_spanned(
            name,
            "namespace must start with '/' and contain no whitespace, query, or fragment",
        ));
    }
    Ok(())
}
fn output(function: &ItemFn) -> Type {
    match &function.sig.output {
        syn::ReturnType::Default => syn::parse_quote!(()),
        syn::ReturnType::Type(_, ty) => *ty.clone(),
    }
}
fn helper(mut path: Path, prefix: &str) -> Path {
    let last = path.segments.last_mut().unwrap();
    last.ident = format_ident!("__socket_{}_{}", prefix, last.ident.unraw());
    path
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Middleware,
    Namespace,
    Event,
    Disconnect,
}
struct Parameters {
    setup: Vec<Tokens>,
    values: Vec<Tokens>,
    payload: Option<Type>,
    event_name: bool,
    auth: Option<Type>,
    session: Option<Type>,
}
impl Parameters {
    fn requirements(&self, server: &Tokens) -> Tokens {
        let slot = |ty: &Option<Type>| match ty {
            Some(ty) => quote!(Some(#server::__private::state_type::<#ty>())),
            None => quote!(None),
        };
        let auth = slot(&self.auth);
        let session = slot(&self.session);
        quote!(#server::__private::StateTypes { auth: #auth, session: #session })
    }
}
fn parameters(function: &mut ItemFn, phase: Phase) -> syn::Result<Parameters> {
    let server = server();
    let mut result = Parameters {
        setup: vec![],
        values: vec![],
        payload: None,
        event_name: false,
        auth: None,
        session: None,
    };
    let mut seen = BTreeSet::new();
    for (index, input) in function.sig.inputs.iter_mut().enumerate() {
        let FnArg::Typed(arg) = input else {
            return Err(syn::Error::new_spanned(input, "export a free function"));
        };
        let mut binding = None;
        for attr in &arg.attrs {
            let Some(id) = attr.path().get_ident() else {
                continue;
            };
            let key = id.to_string();
            if [
                "inject",
                "payload",
                "auth",
                "session",
                "context",
                "event_name",
            ]
            .contains(&key.as_str())
            {
                if binding.is_some() {
                    return Err(syn::Error::new_spanned(
                        attr,
                        "one binding attribute is allowed per argument",
                    ));
                }
                binding = Some((key, attr.clone()));
            }
        }
        let Some((key, attr)) = binding else {
            return Err(syn::Error::new_spanned(
                arg,
                "bind business parameters with #[payload], #[auth], #[session], #[context], #[event_name], or #[inject(name)]",
            ));
        };
        arg.attrs.retain(|a| !a.path().is_ident(&key));
        if key != "inject" && !seen.insert(key.clone()) {
            return Err(syn::Error::new_spanned(attr, "duplicate business binding"));
        }
        if key != "inject" {
            attr.meta.require_path_only()?;
        }
        let ty = &*arg.ty;
        let local = format_ident!("__socket_arg_{index}");
        if key == "inject" {
            let dependency: Ident = attr.parse_args()?;
            if let Type::Reference(reference) = ty {
                if reference.mutability.is_some() {
                    return Err(syn::Error::new_spanned(
                        ty,
                        "shared dependencies cannot be injected mutably",
                    ));
                }
                result.values.push(quote!(&__socket_state.#dependency));
            } else {
                result
                    .values
                    .push(quote!(__socket_state.#dependency.clone()));
            }
            continue;
        }
        if key == "payload" {
            if phase == Phase::Disconnect || matches!(ty, Type::Reference(_)) {
                return Err(syn::Error::new_spanned(
                    ty,
                    "payload must be an owned DTO on a connection, middleware, or event handler",
                ));
            }
            result.payload = Some(ty.clone());
            result.setup.push(if phase == Phase::Event {
                quote!(let #local = #server::__private::payload(__socket_payload.0)?;)
            } else {
                quote!(let #local = __socket_input.payload::<#ty>()?;)
            });
            result.values.push(quote!(#local));
            continue;
        }
        let Type::Reference(reference) = ty else {
            return Err(syn::Error::new_spanned(
                ty,
                "this binding requires an immutable reference",
            ));
        };
        if reference.mutability.is_some() {
            return Err(syn::Error::new_spanned(
                ty,
                "this binding requires an immutable reference",
            ));
        }
        let inner = &*reference.elem;
        match key.as_str() {
            "auth" if phase != Phase::Middleware => {
                result.auth = Some(inner.clone());
                let value = match phase {
                    Phase::Namespace => {
                        quote!(#server::__private::auth_as::<#inner>(__socket_identity.as_ref().ok_or_else(#server::__private::internal_error)?)?)
                    }
                    Phase::Disconnect => quote!(__socket_disconnected.auth::<#inner>()?),
                    _ => quote!(#server::__private::auth::<#inner, _>(&__socket_ref)?),
                };
                result.setup.push(quote!(let #local = #value;));
                result.values.push(quote!(#local.as_ref()));
            }
            "session" if phase == Phase::Event || phase == Phase::Disconnect => {
                result.session = Some(inner.clone());
                let value = if phase == Phase::Disconnect {
                    quote!(__socket_disconnected.session::<#inner>()?)
                } else {
                    quote!(#server::__private::session::<#inner, _>(&__socket_ref)?)
                };
                result.setup.push(quote!(let #local = #value;));
                result.values.push(quote!(#local.as_ref()));
            }
            "context" => {
                let value = match phase {
                    Phase::Namespace | Phase::Middleware => quote!(__socket_input.context()),
                    Phase::Disconnect => quote!(__socket_disconnected.context.clone()),
                    Phase::Event => quote!(#server::__private::context(&__socket_ref)?),
                };
                result.setup.push(quote!(let #local = #value;));
                result.values.push(quote!(&#local));
            }
            "event_name" if phase == Phase::Event => {
                result.event_name = true;
                result.values.push(quote!(__socket_event_name.0.as_str()));
            }
            _ => {
                return Err(syn::Error::new_spanned(
                    attr,
                    "binding unavailable in this handler phase",
                ));
            }
        }
    }
    Ok(result)
}
fn cfgs(function: &ItemFn) -> Vec<syn::Attribute> {
    function
        .attrs
        .iter()
        .filter(|a| a.path().is_ident("cfg") || a.path().is_ident("cfg_attr"))
        .cloned()
        .collect()
}
fn expand_middleware(args: MiddlewareArgs, function: &mut ItemFn) -> syn::Result<Tokens> {
    check_function(function)?;
    let server = server();
    let group = module(args.group);
    let params = parameters(function, Phase::Middleware)?;
    let setup = params.setup;
    let values = params.values;
    let name = &function.sig.ident;
    let vis = &function.vis;
    let adapter = helper(syn::parse_quote!(#name), "authorize");
    let auth_type = helper(syn::parse_quote!(#name), "auth_type");
    let result = output(function);
    let cfg = cfgs(function);
    Ok(quote! {
        #function
        #(#cfg)* #[doc(hidden)] #vis async fn #adapter(__socket_input: &#server::__private::ConnectInput<#group::Adapter>, __socket_state: &::std::sync::Arc<#group::Dependencies>) -> Result<#server::__private::Identity, #server::ClientError> {
            #(#setup)*
            let value = #server::__private::HandlerResult::into_result(#name(#(#values),*).await)?;
            Ok(#server::__private::identity(value))
        }
        #(#cfg)* #[doc(hidden)] #vis fn #auth_type() -> #server::__private::StateType {
            #server::__private::state_type::<<#result as #server::__private::HandlerResult>::Value>()
        }
    })
}
fn export(args: Export, function: &mut ItemFn, event: bool) -> syn::Result<Tokens> {
    check_function(function)?;
    let namespace = if event {
        args.namespace.clone().ok_or_else(|| {
            syn::Error::new_spanned(&args.name, "event requires namespace = \"/...\"")
        })?
    } else {
        if args.namespace.is_some() || args.fallback {
            return Err(syn::Error::new_spanned(
                &args.name,
                "namespace must be the first literal argument",
            ));
        }
        args.name.clone()
    };
    namespace_name(&namespace)?;
    if event
        && (args.name.value().is_empty()
            || [
                "connect",
                "connect_error",
                "disconnect",
                "disconnecting",
                "newListener",
                "removeListener",
            ]
            .contains(&args.name.value().as_str()))
    {
        return Err(syn::Error::new_spanned(
            &args.name,
            "invalid or reserved Socket.IO event name",
        ));
    }
    if event && args.middleware.is_some() {
        return Err(syn::Error::new_spanned(
            &args.name,
            "connect middleware belongs on the namespace export",
        ));
    }
    let server = server();
    let group = module(args.group);
    let params = parameters(
        function,
        if event {
            Phase::Event
        } else {
            Phase::Namespace
        },
    )?;
    let requires = params.requirements(&server);
    let setup = params.setup;
    let values = params.values;
    let name = &function.sig.ident;
    let cfg = cfgs(function);
    let registration = if event {
        let event_name = &args.name;
        let event_key = if args.fallback {
            quote!(None)
        } else {
            quote!(Some(#event_name))
        };
        let payload = params
            .payload
            .map(|ty| quote!(__socket_payload: #server::extract::TryData<#ty>,));
        let event_param = params
            .event_name
            .then(|| quote!(__socket_event_name: #server::extract::Event,));
        let closure = quote! {
            move |__socket_ref: #server::extract::SocketRef<#group::Adapter>, __socket_ack: #server::__private::AckContext<#group::Adapter>, #event_param #payload| {
                let __socket_state = __socket_state.clone();
                async move {
                    let __observation = __socket_metrics.handler.start();
                    let __result: Result<_, #server::ClientError> = async {
                        #server::__private::ensure_active(&__socket_ref)?;
                        #(#setup)*
                        let value = #server::__private::HandlerResult::into_result(#name(#(#values),*).await)?;
                        Ok(#server::__private::event_reply(value))
                    }.await;
                    __observation.finish(__result.is_ok());
                    #server::__private::complete_event(&__socket_ref, __socket_ack, __result, __socket_metrics);
                }
            }
        };
        let bind = if args.fallback {
            quote!(__socket.on_fallback(#closure);)
        } else {
            quote!(__socket.on(#event_name, #closure);)
        };
        quote! {
            #group::Event(#server::__private::EventRegistration {
                namespace: #namespace, event: #event_key, requires: || #requires,
                register: |__socket, __socket_state, __socket_metrics| { #bind },
            })
        }
    } else {
        let (authenticate, auth_type) = if let Some(path) = args.middleware {
            let adapter = helper(path.clone(), "authorize");
            let metadata = helper(path, "auth_type");
            (
                quote!(Some(#adapter(&__socket_input, &__socket_state).await?)),
                quote!(Some(#metadata())),
            )
        } else {
            (quote!(None), quote!(None))
        };
        let result = output(function);
        quote! {
            #group::Namespace(#server::__private::NamespaceRegistration {
                namespace: #namespace, requires: || #requires,
                provides: || #server::__private::StateTypes {
                    auth: #auth_type,
                    session: Some(#server::__private::state_type::<<<#result as #server::__private::HandlerResult>::Value as #server::__private::IntoAccept>::Session>()),
                },
                register: |__io, __socket_state, __socket_bindings, __socket_metrics| {
                    let __socket_bindings = ::std::sync::Arc::new(__socket_bindings);
                    let middleware = move |__socket_input: #server::__private::ConnectInput<#group::Adapter>| {
                        let __socket_state = __socket_state.clone(); let __socket_bindings = __socket_bindings.clone();
                        async move {
                            let __observation = __socket_metrics.admission.start();
                            let result: Result<(), #server::ClientError> = async {
                                let __socket_identity: Option<#server::__private::Identity> = #authenticate;
                                #(#setup)*
                                let value = #server::__private::HandlerResult::into_result(#name(#(#values),*).await)?;
                                let accept = #server::__private::IntoAccept::into_accept(value);
                                let __socket_ref = __socket_input.socket();
                                __socket_bindings.bind(&__socket_ref, __socket_state.clone());
                                let hook = __socket_bindings.disconnect(__socket_state.clone());
                                #server::__private::prepare(&__socket_ref, __socket_identity, accept, __socket_metrics, hook);
                                Ok(())
                            }.await;
                            __observation.finish(result.is_ok());
                            result
                        }
                    };
                    __io.ns(#namespace, #server::__private::namespace_handler(middleware, __socket_metrics))
                },
            })
        }
    };
    Ok(
        quote! { #function #(#cfg)* const _: () = { #server::__private::inventory::submit! { #registration } }; },
    )
}
fn expand_disconnect(args: DisconnectArgs, function: &mut ItemFn) -> syn::Result<Tokens> {
    check_function(function)?;
    namespace_name(&args.namespace)?;
    let server = server();
    let group = module(args.group);
    let params = parameters(function, Phase::Disconnect)?;
    let requires = params.requirements(&server);
    let setup = params.setup;
    let values = params.values;
    let name = &function.sig.ident;
    let namespace = args.namespace;
    let cfg = cfgs(function);
    Ok(quote! {
        #function #(#cfg)* const _: () = { #server::__private::inventory::submit! {
            #group::Disconnect(#server::__private::DisconnectRegistration {
                namespace: #namespace, requires: || #requires,
                invoke: |__socket_disconnected, __socket_state| Box::pin(async move {
                    let result: Result<(), #server::ClientError> = async {
                        #(#setup)*
                        let _: () = #server::__private::HandlerResult::into_result(#name(#(#values),*).await)?;
                        Ok(())
                    }.await;
                    #server::__private::disconnect_result(result);
                }),
            })
        } };
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validates_registration_and_export_boundaries() {
        for source in [
            "dependencies(app: String, app: u64)",
            "group = a, group = b",
            "adapter = One, adapter = Two",
        ] {
            assert!(syn::parse_str::<Registry>(source).is_err(), "{source}");
        }
        for (source, event) in [
            (r#""echo""#, true),
            (r#""connect", namespace = "/""#, true),
            (r#""/missing#fragment""#, false),
            (r#""echo", namespace = "/", middleware = auth"#, true),
        ] {
            let args = syn::parse_str::<Export>(source).unwrap();
            let mut function = syn::parse_quote!(
                async fn handler() {}
            );
            assert!(export(args, &mut function, event).is_err(), "{source}");
        }
    }
    #[test]
    fn rejects_protocol_extractors_and_invalid_business_bindings() {
        for source in [
            "async fn handler(socket: SocketRef) {}",
            "async fn handler(#[payload] a: String, #[payload] b: u64) {}",
            "async fn handler(#[auth] user: User) {}",
            "async fn handler(#[session] state: &mut State) {}",
        ] {
            let mut function = syn::parse_str::<ItemFn>(source).unwrap();
            assert!(parameters(&mut function, Phase::Event).is_err(), "{source}");
        }
    }
}
