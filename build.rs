fn main() {
    cc::Build::new()
        .file("src/context/libcontext.c")
        .compile("libcontext");

    let context_bindings = bindgen::Builder::default()
        .header("src/context/libcontext.h")
        .generate()
        .unwrap();
    context_bindings
        .write_to_file("src/context/libcontext.rs")
        .unwrap();

    let aio_bindings = bindgen::Builder::default()
        .header("src/io/wrapper.h")
        .generate()
        .unwrap();
    aio_bindings.write_to_file("src/io/aio.rs").unwrap();
}
