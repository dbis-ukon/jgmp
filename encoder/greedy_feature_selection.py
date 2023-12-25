
from typing import Set
import numpy as np


# This is an implementation of the algorithm presented by Farahat et al. in "Efficient Greedy Feature Selection for Unsupervised Learning"


def greedy_feature_selection(A: np.ndarray, k: int, regularization: float = 10e-5, old_sample_length: int = 0) -> Set[int]:
    shape = A.shape
    assert(len(shape) == 2)
    m, n = shape

    S = set()
    f = np.zeros((k + 1, n))
    g = np.zeros((k + 1, n))
    omega = np.zeros((k, n))

    ATA = np.dot(A.T, A)

    for i in range(n):
        f[0, i] = np.linalg.norm(ATA[:, i]) ** 2
        g[0, i] = np.linalg.norm(A[:, i]) ** 2

    for t in range(1, k + 1):
        if np.max(f[t - 1, :]) <= 10**-10:
            # if all values in score are 0, return S
            return S
        score = f[t - 1, :] / (g[t - 1, :] + regularization)
        if t <= old_sample_length:
            l = t - 1
        else:
            l = np.argmax(score)
            if l in S or np.max(score) <= 10**-10:
                return S
        S.add(l)
        delta = A.T @ A[:, l] - np.sum(omega[:t - 1, l, None] * omega[:t - 1, :], axis=0)
        omega[t - 1, :] = delta / np.linalg.norm(delta[l]) ** 0.5
        omega_sum = np.zeros(n)
        for i in range(t - 1):
            omega_sum += np.dot(np.outer(omega[i, :], omega[t - 1, :]), omega[i, :])
        f[t, :] = f[t - 1, :] - 2 * (omega[t - 1, :] * (np.dot(ATA, omega[t - 1, :]) - omega_sum)) + np.linalg.norm(omega[t - 1, :]) ** 2 * (omega[t - 1, :] * omega[t - 1, :])
        g[t, :] = g[t - 1, :] - (omega[t - 1, :] * omega[t - 1, :])

    return S



def test_feature_selection():
    A = np.array([[1, 0, 0, 0, 0, 0],
                  [1, 0, 0, 0, 0, 0],
                  [1, 0, 0, 0, 0, 0],
                  [1, 0, 0, 0, 0, 0],
                  [0, 1, 1, 0, 0, 0],
                  [1, 0, 1, 1, 0, 1]])
    k = 4
    S = greedy_feature_selection(A, k)
    print(S)


# test_feature_selection()
