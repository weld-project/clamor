all:
	cargo build --release --features=serialize
	mv target/release/libkdtree.so target/release/libkdtree-malloc.so
	cargo build --release --features="serialize clamor"
	mv target/release/libkdtree.so target/release/libkdtree-rsmalloc.so

