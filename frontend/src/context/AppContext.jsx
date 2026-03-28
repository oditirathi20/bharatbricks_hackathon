import { useMemo, useState } from "react"
import { AppContext } from "./appContextStore"

export function AppProvider({ children }) {
  const [citizenId, setCitizenId] = useState("")
  const [selectedCategory, setSelectedCategory] = useState("")
  const [answers, setAnswers] = useState({})
  const [profile, setProfile] = useState(null)
  const [results, setResults] = useState([])

  const resetJourney = () => {
    setSelectedCategory("")
    setAnswers({})
    setProfile(null)
    setResults([])
  }

  const value = useMemo(
    () => ({
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
    [citizenId, selectedCategory, answers, profile, results],
  )

  return <AppContext.Provider value={value}>{children}</AppContext.Provider>
}
