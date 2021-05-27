{ pkgs ? import <nixpkgs> {}}:
let
  nixPackages = with pkgs.python38Packages; [
    pyspark
    pylint
  ];
in
pkgs.stdenv.mkDerivation {
  name = "spark-dev-env";
  nativeBuildInputs = nixPackages;
  # shellHook = ''
  # '';
}
