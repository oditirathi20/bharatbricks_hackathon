import { useMemo, useState } from "react"
import { AppContext } from "./appContextStore"

const LOCAL_LANGUAGE_KEY = "adhikar_aina_language"

function readInitialLanguage() {
  return localStorage.getItem(LOCAL_LANGUAGE_KEY) || "en"
}

export function AppProvider({ children }) {
  const [language, setLanguageState] = useState(readInitialLanguage)
  const [citizenId, setCitizenId] = useState("")
  const [selectedCategory, setSelectedCategory] = useState("")
  const [answers, setAnswers] = useState({})
  const [profile, setProfile] = useState(null)
  const [results, setResults] = useState([])

  const setLanguage = (nextLanguage) => {
    setLanguageState(nextLanguage)
    localStorage.setItem(LOCAL_LANGUAGE_KEY, nextLanguage)
  }

  const resetJourney = () => {
    setSelectedCategory("")
    setAnswers({})
    setProfile(null)
    setResults([])
  }

  const value = useMemo(
    () => ({
      language,
      setLanguage,
      citizenId,
      setCitizenId,
      selectedCategory,
      setSelectedCategory,
      answers,
      setAnswers,
      profile,
      setProfile,
      results,
      setResults,
      resetJourney,
    }),
    [language, citizenId, selectedCategory, answers, profile, results],
  )

  return <AppContext.Provider value={value}>{children}</AppContext.Provider>
}
