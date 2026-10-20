{
  description = "Ruvio server packages and development shell";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    { nixpkgs, rust-overlay, ... }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
      ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
      perSystem = forAllSystems (
        system:
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [ rust-overlay.overlays.default ];
          };
          toolchainSpec = builtins.fromTOML (builtins.readFile ./rust-toolchain.toml);
          toolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
          rustPlatform = pkgs.makeRustPlatform {
            cargo = toolchain;
            rustc = toolchain;
          };
          # Use Nix's compiler wrapper explicitly. build.rs then skips the
          # symbol-ordering flags intended for Rust's default Linux linker.
          linkerEnv = pkgs.lib.optionalAttrs (system == "x86_64-linux") {
            CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER = "${pkgs.stdenv.cc}/bin/cc";
          };
          ruvio = rustPlatform.buildRustPackage {
            pname = "ruvio";
            version = (builtins.fromTOML (builtins.readFile ./Cargo.toml)).package.version;
            src = pkgs.lib.cleanSource ./.;
            cargoLock.lockFile = ./Cargo.lock;
            nativeBuildInputs = [ pkgs.cmake ];
            env = linkerEnv;
            cargoBuildFlags = [ "--bins" ];
            checkType = "debug";
            # Live integration and crash suites remain explicit shell commands.
            cargoTestFlags = [
              "--lib"
              "--bins"
              "--test"
              "layout"
              "--test"
              "zero_alloc"
            ];
            postInstall = ''
              install -Dm644 ruvio.example.toml "$out/share/ruvio/ruvio.example.toml"
              install -Dm644 LICENSE "$out/share/licenses/ruvio/LICENSE"
              install -Dm644 licenses/THIRD_PARTY_NOTICES "$out/share/licenses/ruvio/THIRD_PARTY_NOTICES"
            '';
            meta = {
              description = "Memory-first key-value server with a tested RESP subset";
              homepage = "https://ruviodb.com/";
              license = pkgs.lib.licenses.agpl3Only;
              mainProgram = "ruvio";
              platforms = systems;
            };
          };
        in
        {
          packages = {
            inherit ruvio;
            default = ruvio;
          };
          devShells.default = pkgs.mkShell (
            {
              packages = [
                (toolchain.override {
                  extensions = toolchainSpec.toolchain.components ++ [
                    "rust-src"
                    "rust-analyzer"
                  ];
                })
                pkgs.cmake
                pkgs.python3
                pkgs.git
                pkgs.bash
                pkgs.coreutils
                pkgs.gnugrep
                pkgs.gnused
                pkgs.gawk
                pkgs.cargo-nextest
                pkgs.redis
              ];
            }
            // linkerEnv
          );
          checks = {
            package = ruvio;
            formatting = pkgs.runCommand "ruvio-formatting" { nativeBuildInputs = [ toolchain ]; } ''
              cd ${ruvio.src}
              cargo fmt --all --check
              touch "$out"
            '';
          };
          formatter = pkgs.nixfmt;
        }
      );
    in
    {
      packages = forAllSystems (system: perSystem.${system}.packages);
      devShells = forAllSystems (system: perSystem.${system}.devShells);
      checks = forAllSystems (system: perSystem.${system}.checks);
      formatter = forAllSystems (system: perSystem.${system}.formatter);
    };
}
