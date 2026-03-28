import { useMemo, useState } from "react"
import { LanguageContext } from "./languageStore"

const DEFAULT_LANGUAGE = "en"
const STORAGE_KEY = "app_language"

function getInitialLanguage() {
  return localStorage.getItem(STORAGE_KEY) || DEFAULT_LANGUAGE
}

export function LanguageProvider({ children }) {
  const [language, setLanguage] = useState(getInitialLanguage)

  const changeLanguage = (nextLanguage) => {
    setLanguage(nextLanguage)
    localStorage.setItem(STORAGE_KEY, nextLanguage)
  }

  const value = useMemo(
    () => ({
      language,
      changeLanguage,
    }),
    [language],
  )

  return <LanguageContext.Provider value={value}>{children}</LanguageContext.Provider>
}
