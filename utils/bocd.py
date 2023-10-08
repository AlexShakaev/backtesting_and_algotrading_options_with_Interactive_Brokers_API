# -*- coding: utf-8 -*-
"""
# Code initally borrowed from:
# https://github.com/hildensia/bayesian_changepoint_detection
# under the MIT license.
"""

import numpy as np
from scipy import stats
from functools import partial 
import matplotlib.pyplot as plt

def constant_hazard(lam, r):
    return 1/lam * np.ones(r.shape)


class StudentT:
    def __init__(self, alpha, beta, kappa, mu):
        self.alpha0 = self.alpha = np.array([alpha])
        self.beta0 = self.beta = np.array([beta])
        self.kappa0 = self.kappa = np.array([kappa])
        self.mu0 = self.mu = np.array([mu])

    def pdf(self, data):
        return stats.t.pdf(x=data, 
                           df=2*self.alpha,
                           loc=self.mu,
                           scale=np.sqrt(self.beta * (self.kappa+1) / (self.alpha *
                               self.kappa)))

    def update_theta(self, data):
        muT0 = np.concatenate((self.mu0, (self.kappa * self.mu + data) / (self.kappa + 1)))
        kappaT0 = np.concatenate((self.kappa0, self.kappa + 1.))
        alphaT0 = np.concatenate((self.alpha0, self.alpha + 0.5))
        betaT0 = np.concatenate((self.beta0, self.beta + (self.kappa * (data -
            self.mu)**2) / (2. * (self.kappa + 1.))))
            
        self.mu = muT0
        self.kappa = kappaT0
        self.alpha = alphaT0
        self.beta = betaT0
        

class BOCD:
    def __init__(self, hazard_function, observation_likelihood, length):   
        self.t = 0
        self.R = np.zeros((length, length))
        self.H = hazard_function
        self.observation_likelihood = observation_likelihood
        self.R[0, 0] = 1     
        self.changepoints = []
        self.cp_probs = []    
        self.length = length
        self.cp_detected = False
    
    def expand_matrix(self):
        L = self.R.shape[0]    
        self.R = np.pad(self.R, ((0,L),(0,L)))
        self.length  = self.R.shape[0]
        
    def update(self, x):   
        self.cp_detected = False
        
        if self.t == self.length - 1:
            self.expand_matrix()
                      
        t  = self.t
        
        predprobs = np.round(self.observation_likelihood.pdf(x), 16)
        # Evaluate the hazard function for this interval
        H = self.H(np.array(range(t + 1)))

        # Evaluate the growth probabilities - shift the probabilities down and to
        # the right, scaled by the hazard function and the predictive
        # probabilities.
        self.R[1 : t + 2, t + 1] = self.R[0 : t + 1, t] * predprobs * (1 - H)

        # Evaluate the probability that there *was* a changepoint and we're
        # accumulating the mass back down at r = 0.
        self.R[0, t + 1] = np.sum(self.R[0    : t + 1, t] * predprobs * H)

        # Renormalize the run length probabilities for improved numerical
        # stability.
        self.R[:, t + 1] = self.R[:, t + 1] / np.sum(self.R[:, t + 1])
        
        # Update the parameter sets for each possible run length.
        self.observation_likelihood.update_theta(x)    
        
        cp = self.R[1, t]
        self.cp_probs.append(cp)                         
        if cp > 0.35:
            if not t == 1:
                self.changepoints.append(t)
                self.cp_detected = True
                print(f'cp detected at index {self.t}, value {x}')                   
        
        self.t += 1                                
        
        
def generate_normal_time_series(num, minl=50, maxl=1000):
    data = np.array([], dtype=np.float64)
    partition = np.random.randint(minl, maxl, num)
    for p in partition:
        mean = np.random.randn()*10
        var = np.random.randn()*1
        if var < 0:
            var = var * -1
        tdata = np.random.normal(mean, var, p)
        data = np.concatenate((data, tdata))
    return data
          
if __name__ == "__main__":
    
    data = generate_normal_time_series(3)

    lambda_ = 150
    alpha = 1
    beta  = 1
    kappa = 1
    mu    = 0  
    bocd = BOCD(partial(constant_hazard, lambda_),
                  StudentT(alpha, beta, kappa, mu), lambda_)
    for x in data:
        bocd.update(x)        

    
    fig, ax = plt.subplots(figsize=[18, 16])
    ax = fig.add_subplot(2, 1, 1)
    ax.plot(data)
    [ax.axvline(x=i,color='r') for i in bocd.changepoints]
    ax = fig.add_subplot(2, 1, 2, sharex=ax)
  
    Nw=15;
    ax.plot(bocd.R[Nw,Nw:-1])  
    plt.show()
    
