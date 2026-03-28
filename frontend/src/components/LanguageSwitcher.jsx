import { useLanguage } from "../i18n/useLanguage"

const LANGUAGE_OPTIONS = [
  { code: "en", label: "EN" },
  { code: "hi", label: "हिं" },
  { code: "mr", label: "मर" },
]

function LanguageSwitcher() {
  const { language, changeLanguage } = useLanguage()

  return (
    <div className="inline-flex items-center gap-1 rounded-full border border-slate-200 bg-white/90 p-1 shadow-sm">
      {LANGUAGE_OPTIONS.map((languageOption) => (
        <button
          key={languageOption.code}
          type="button"
          onClick={() => changeLanguage(languageOption.code)}
          className={`rounded-full px-3 py-1.5 text-xs font-semibold transition ${
            language === languageOption.code
              ? "bg-slate-900 text-white shadow"
              : "text-slate-500 hover:bg-slate-100"
          }`}
        >
          {languageOption.label}
        </button>
      ))}
    </div>
  )
}

export default LanguageSwitcher
