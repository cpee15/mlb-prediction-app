import React from 'react'
import { BrowserRouter, Routes, Route, NavLink, useParams } from 'react-router-dom'
import './styles/bet105-mobile.css'
import HomePage from './pages/HomePage'
import LandingV2Page from './pages/LandingV2Page'
import MatchupDetailPage from './pages/MatchupDetailPage'
import PitcherPage from './pages/PitcherPage'
import RollingPitcherPage from './pages/RollingPitcherPage'
import TeamPage from './pages/TeamPage'
import StandingsPage from './pages/StandingsPage'
import CompetitiveAnalysisPage from './pages/CompetitiveAnalysisPage'
import YesterdayTodayPage from './pages/YesterdayTodayPage'
import AIPage from './pages/AIPage'
import LiveScoreboardPage from './pages/LiveScoreboardPage'
import LiveGamePageRestored from './pages/LiveGamePageRestored'
import DailyOddsPage from './pages/DailyOddsPage'
import Bet105SportsbookPage from './pages/Bet105SportsbookPage'
import ModelProjectionsPage from './pages/ModelProjectionsPage'
import NewsPageClean from './pages/NewsPageClean'
import MyDashboardReportBuilderPage from './pages/MyDashboardReportBuilderPage'
import ModelTrackerPage from './pages/ModelTrackerPage'

// Set VITE_ENABLE_BATTER_PAGE=true in Railway env vars to re-enable the Batter routes.
// Keep false until the leaderboard endpoint is validated stable in production.
const ENABLE_BATTER_PAGE = import.meta.env.VITE_ENABLE_BATTER_PAGE === 'true'

const BatterPage = ENABLE_BATTER_PAGE ? React.lazy(() => import('./pages/BatterPage')) : null
const RollingBatterPage = ENABLE_BATTER_PAGE ? React.lazy(() => import('./pages/RollingBatterPage')) : null

function BatterTemporarilyUnavailable() {
  return (
    <section className="state-panel">
      <div className="status-badge warning" style={{ marginBottom: 12 }}>Temporarily Disabled</div>
      <h1 className="page-title" style={{ fontSize: 24 }}>Batter dashboard validation in progress</h1>
      <p className="page-subtitle" style={{ margin: '10px auto 0' }}>
        The Batter dashboard is temporarily unavailable while the backend leaderboard endpoint is validated for production stability. Matchups, pitchers, teams, odds, live scores, and model projections remain available.
      </p>
    </section>
  )
}

function MatchupRoute() {
  const { game_pk } = useParams()
  return <MatchupDetailPage key={game_pk} />
}

const navLinkClass = ({ isActive }) => `app-nav-link${isActive ? ' active' : ''}`

export default function App() {
  return (
    <BrowserRouter>
      <div className="app-shell">
        <nav className="app-nav" aria-label="Primary navigation">
          <NavLink to="/" className="app-brand">
            <span className="app-brand-mark">◆</span>
            <span>MLB Prediction Engine</span>
          </NavLink>
          <NavLink to="/" end className={navLinkClass}>Matchups</NavLink>
          <NavLink to="/daily-odds" className={navLinkClass}>Daily Odds</NavLink>
          <NavLink to="/sportsbook/bet105" className={navLinkClass}>Bet105 Sportsbook</NavLink>
          <NavLink to="/news" className={navLinkClass}>News</NavLink>
          <NavLink to="/models/projections" className={navLinkClass}>Model Projections</NavLink>
          <NavLink to="/my-dashboard" className={navLinkClass}>My Dashboard</NavLink>
          <NavLink to="/standings" className={navLinkClass}>Standings</NavLink>
          <NavLink to="/pitcher" className={navLinkClass}>Pitcher</NavLink>
          <NavLink to="/batter" className={navLinkClass}>Batter</NavLink>
          <NavLink to="/team" className={navLinkClass}>Team</NavLink>
          <NavLink to="/calendar" className={navLinkClass}>Calendar</NavLink>
          <NavLink to="/ai-data-assistant" className={navLinkClass}>AI Data Assistant</NavLink>
          <NavLink to="/live" className={navLinkClass}>Live</NavLink>
          <NavLink to="/model-tracker" className={navLinkClass}>Model Tracker</NavLink>
        </nav>
        <main className="app-main">
          <Routes>
            <Route path="/" element={<HomePage />} />
            <Route path="/landing-v2" element={<LandingV2Page />} />
            <Route path="/daily-odds" element={<DailyOddsPage />} />
            <Route path="/sportsbook/bet105" element={<Bet105SportsbookPage />} />
            <Route path="/news" element={<NewsPageClean />} />
            <Route path="/models/projections" element={<ModelProjectionsPage />} />
            <Route path="/model-tracker" element={<ModelTrackerPage />} />
            <Route path="/my-dashboard" element={<MyDashboardReportBuilderPage />} />
            <Route path="/matchup/:game_pk" element={<MatchupRoute />} />
            <Route path="/matchup/:game_pk/competitive" element={<CompetitiveAnalysisPage />} />
            <Route path="/standings" element={<StandingsPage />} />
            <Route path="/pitcher" element={<PitcherPage />} />
            <Route path="/pitcher/:id" element={<PitcherPage />} />
            <Route path="/pitcher/:id/rolling" element={<RollingPitcherPage />} />
            <Route path="/batter" element={ENABLE_BATTER_PAGE ? <React.Suspense fallback={null}><BatterPage /></React.Suspense> : <BatterTemporarilyUnavailable />} />
            <Route path="/batter/:id" element={ENABLE_BATTER_PAGE ? <React.Suspense fallback={null}><BatterPage /></React.Suspense> : <BatterTemporarilyUnavailable />} />
            <Route path="/batter/:id/rolling" element={ENABLE_BATTER_PAGE ? <React.Suspense fallback={null}><RollingBatterPage /></React.Suspense> : <BatterTemporarilyUnavailable />} />
            <Route path="/team" element={<TeamPage />} />
            <Route path="/team/:id" element={<TeamPage />} />
            <Route path="/calendar" element={<YesterdayTodayPage />} />
            <Route path="/ai" element={<AIPage />} />
            <Route path="/ai-data-assistant" element={<AIPage />} />
            <Route path="/live" element={<LiveScoreboardPage />} />
            <Route path="/live/:game_pk" element={<LiveGamePageRestored />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  )
}
