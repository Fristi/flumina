import scalariform.formatter.preferences._

object Settings {
  lazy val commonFormattingPreferences = FormattingPreferences()
    .setPreference(AlignSingleLineCaseStatements, true)
    .setPreference(DanglingCloseParenthesis, Preserve)
    .setPreference(RewriteArrowSymbols, false)
    .setPreference(AlignParameters, true)
    .setPreference(DoubleIndentClassDeclaration, true)
    .setPreference(AlignSingleLineCaseStatements.MaxArrowIndent, 90)
}