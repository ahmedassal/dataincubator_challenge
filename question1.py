import numpy as np
from scipy import stats as st

__author__ = 'Ahmed Assal'
import numpy
diceOutcome = (1,2,3,4,5,6)
M = (20,10000)

for m in M:
    print("\nFor M = ", m, "\n")
    cumSum = []
    rollsOutcome = []
    summation = 0
    rollsNums = []
    rollNum = 1
    while summation<m:
        rollsOutcome.append(np.random.choice(diceOutcome, replace = False))
        summation = sum(rollsOutcome)
        # if(summation< m):
        cumSum.append(summation)
        rollsNums.append(rollNum)
        rollNum += 1

    rollsOutcome = np.array(rollsOutcome)
    cumSum = np.array(cumSum)
    cumSumMinusM = cumSum - m
    rollsNums= np.array(rollsNums)

    print("Rolls outcome = ", rollsOutcome)
    print("CumSum = ", cumSum)
    print("CumSum-M = ", cumSumMinusM)
    print("mean(CumSum-M) = ", cumSumMinusM.mean())
    print("std(CumSum-M) = ", cumSumMinusM.std())

    print("Number of rolls = ", rollsNums)
    print("mean(Number of rolls) = ", rollsNums.mean())
    print("std(Number of rolls) = ", rollsNums.std())


    # print(cumSum.std(), cumSum.var(), cumSum.mean())
    # print(st.describe(cumSum))