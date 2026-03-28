import axios from "axios"
import { buildMockSchemes } from "../data/schemes"

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://127.0.0.1:8000"

export async function registerCitizen(payload) {
  try {
    const response = await axios.post(`${API_BASE_URL}/api/register-user`, payload, {
      timeout: 8000,
      headers: { "Content-Type": "application/json" },
    })

    return { ok: true, data: response.data }
  } catch (error) {
    // Fail open in demos so the UI can continue with mock data.
    return {
      ok: false,
      error: error?.message || "register_failed",
    }
  }
}

export async function getEligibilityResults(citizenId, profile) {
  try {
    const response = await axios.get(`${API_BASE_URL}/api/get-results/${citizenId}`, {
      timeout: 10000,
    })

    const schemes = response?.data?.schemes || []
    return { ok: true, schemes }
  } catch (error) {
    return {
      ok: false,
      schemes: buildMockSchemes(profile),
      error: error?.message || "results_failed",
    }
  }
}
