{
  description = "Python project with nix + direnv + poetry";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

  outputs = { self, nixpkgs, ... }:
  let
    supportedSystems = [ "aarch64-darwin" "x86_64-linux" ];
    forEachSystem = systems: f:
      nixpkgs.lib.genAttrs systems (system:
        f { pkgs = import nixpkgs { inherit system; }; });
  in {
    devShells = forEachSystem supportedSystems ({ pkgs }: {
      default = pkgs.mkShell {
        buildInputs = [
          pkgs.python313
        ];
      };
    });
  };
}
