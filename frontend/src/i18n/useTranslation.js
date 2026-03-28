import { useLanguage } from "./useLanguage"
import { translations } from "./translations"

function getValueByPath(object, path) {
  return path.split(".").reduce((current, key) => (current ? current[key] : undefined), object)
}

export function useTranslation() {
  const { language } = useLanguage()

  const t = (key, params = {}) => {
    const selectedLanguagePack = translations[language] || translations.en
    const fallbackValue = getValueByPath(translations.en, key)
    const rawValue = getValueByPath(selectedLanguagePack, key) ?? fallbackValue ?? key

    if (typeof rawValue !== "string") {
      return key
    }

    return rawValue.replace(/\{\{(\w+)\}\}/g, (_, variable) => String(params[variable] ?? ""))
  }

  return {
    language,
    t,
  }
}
