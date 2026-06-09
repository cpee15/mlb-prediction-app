import assert from 'node:assert/strict'
import test from 'node:test'

import {
  americanOddsToDecimal,
  americanOddsToImpliedProbability,
  calculateEdge,
  calculateEvPer100,
  probabilityToFairAmericanOdds,
} from './selectBetOfTheDay.mjs'

const near = (actual, expected, epsilon = 0.000001) => assert.ok(Math.abs(actual - expected) < epsilon, `${actual} not within ${epsilon} of ${expected}`)

test('+100 implied probability = 50%', () => {
  near(americanOddsToImpliedProbability(100), 0.5)
})

test('+150 implied probability = 40%', () => {
  near(americanOddsToImpliedProbability(150), 0.4)
})

test('-150 implied probability = 60%', () => {
  near(americanOddsToImpliedProbability(-150), 0.6)
})

test('model 55%, market 50% edge = 5%', () => {
  near(calculateEdge(0.55, 0.5), 0.05)
})

test('fair American odds from model probability', () => {
  near(probabilityToFairAmericanOdds(0.55), -122.2222222222, 0.0001)
  near(probabilityToFairAmericanOdds(0.4), 150)
})

test('decimal odds for positive and negative American odds', () => {
  near(americanOddsToDecimal(150), 2.5)
  near(americanOddsToDecimal(-150), 1.6666666667, 0.0001)
})

test('EV calculation works for positive odds', () => {
  near(calculateEvPer100(0.55, 150), 37.5)
})

test('EV calculation works for negative odds', () => {
  near(calculateEvPer100(0.65, -150), 8.3333333333, 0.0001)
})
