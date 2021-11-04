all:
	cargo build --release --features=serialize
	mv target/release/libclamor_hashmap.so target/release/libclamor_hashmap-malloc.so
	cargo build --release --features="clamor"
	mv target/release/libclamor_hashmap.so target/release/libclamor_hashmap-rsmalloc.so

