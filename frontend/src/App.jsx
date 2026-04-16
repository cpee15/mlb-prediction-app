import React from 'react'
import { BrowserRouter, Routes, Route, NavLink } from 'react-router-dom'
import HomePage from './pages/HomePage'
import PitcherPage from './pages/PitcherPage'
import BatterPage from './pages/BatterPage'
import TeamPage from './pages/TeamPage'

const styles = {
  nav: {
    background: '#161b22',
    borderBottom: '1px solid #30363d',
    padding: '0 24px',
    display: 'flex',
    alignItems: 'center',
    gap: '32px',
    height: '56px',
  },
  brand: {
    fontSize: '18px',
    fontWeight: '700',
    color: '#58a6ff',
    textDecoration: 'none',
    letterSpacing: '-0.5px',
  },
  link: {
    color: '#8b949e',
    textDecoration: 'none',
    fontSize: '14px',
    fontWeight: '500',
    padding: '4px 0',
    borderBottom: '2px solid transparent',
    transition: 'color 0.15s, border-color 0.15s',
  },
  activeLink: {
    color: '#e6edf3',
    borderBottomColor: '#58a6ff',
  },
  main: {
    maxWidth: '1100px',
    margin: '0 auto',
    padding: '32px 24px',
  },
}

export default function App() {
  return (
    <BrowserRouter>
      <nav style={styles.nav}>
        <NavLink to="/" style={styles.brand}>⚾ MLB Predict</NavLink>
        <NavLink to="/" style={({ isActive }) => ({ ...styles.link, ...(isActive ? styles.activeLink : {}) })}>
          Matchups
        </NavLink>
        <NavLink to="/pitcher" style={({ isActive }) => ({ ...styles.link, ...(isActive ? styles.activeLink : {}) })}>
          Pitcher
        </NavLink>
        <NavLink to="/batter" style={({ isActive }) => ({ ...styles.link, ...(isActive ? styles.activeLink : {}) })}>
          Batter
        </NavLink>
        <NavLink to="/team" style={({ isActive }) => ({ ...styles.link, ...(isActive ? styles.activeLink : {}) })}>
          Team
        </NavLink>
      </nav>
      <main style={styles.main}>
        <Routes>
          <Route path="/" element={<HomePage />} />
          <Route path="/pitcher" element={<PitcherPage />} />
          <Route path="/pitcher/:id" element={<PitcherPage />} />
          <Route path="/batter" element={<BatterPage />} />
          <Route path="/batter/:id" element={<BatterPage />} />
          <Route path="/team" element={<TeamPage />} />
        </Routes>
      </main>
    </BrowserRouter>
  )
}
