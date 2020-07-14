import numpy as np


class EmpDist:
    def __init__(self, a):
        self.a = np.array(a)

    def rvs(self, n):
        return np.random.choice(self.a, size=n, replace=True)


class BootstrapStat:
    def __init__(self, a, stat=np.median, replicate_size=None):
        self.a = np.array(a)
        self.stat = stat
        self.replicate_size = replicate_size or len(a)

    def rvs(self, n):
        return np.random.choice(self.a, size=n, replace=True)

    def rvs2d(self, n, m):
        return np.random.choice(self.a, size=(n, m), replace=True)

    def draw_replicate(self):
        samps = self.rvs(n=self.replicate_size)
        return self.stat(samps)

    def _draw_replicates_slow(self, n_reps):
        res = [self.draw_replicate() for _ in range(n_reps)]
        return np.array(res)

    def draw_replicates(self, n_reps):
        rvs = self.rvs2d(self.replicate_size, n_reps)
        res = self.stat(rvs, axis=0)
        assert len(res) == n_reps
        return res
