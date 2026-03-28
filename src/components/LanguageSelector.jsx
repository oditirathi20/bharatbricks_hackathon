import { LANGUAGE_OPTIONS } from "../data/questions"

function LanguageSelector({ value, onChange, className = "" }) {
  return (
    <div className={`flex items-center gap-2 ${className}`}>
      <label htmlFor="language" className="text-sm font-semibold text-slate-700">
        Language
      </label>
      <select
        id="language"
        value={value}
        onChange={(event) => onChange(event.target.value)}
        className="rounded-xl border border-slate-300 bg-white px-3 py-2 text-sm font-medium text-slate-700 shadow-sm focus:border-teal-500 focus:outline-none"
      >
        {LANGUAGE_OPTIONS.map((languageOption) => (
          <option key={languageOption.code} value={languageOption.code}>
            {languageOption.label}
          </option>
        ))}
      </select>
    </div>
  )
}

export default LanguageSelector
