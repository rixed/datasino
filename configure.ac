AC_INIT(datasino, 0.10.0)
m4_include([m4/ocaml.m4])

AC_ARG_VAR(OCAMLFLAGS, [Additional flags to pass to the ocaml compiler])
AC_ARG_VAR(OCAMLCFLAGS, [Additional flags to pass to the ocaml bytecode compiler])

AC_PROG_OCAML
AS_IF(
  [test "$OCAMLC" = no],
  AC_MSG_ERROR([You must install the OCaml compiler.]))

AC_PROG_FINDLIB
AS_IF(
  [test "$OCAMLFIND" = no],
  AC_MSG_ERROR([You must install OCaml findlib (the ocamlfind command).]))

AC_CHECK_OCAML_PKG(batteries)
AC_CHECK_OCAML_PKG(cmdliner)
AC_CHECK_OCAML_PKG(dessser)
AC_CHECK_OCAML_PKG(kafka)

AC_CONFIG_FILES([
  Makefile
  opam
  docker/Dockerfile
])
AC_OUTPUT
