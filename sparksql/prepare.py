
n = [1,3,0]
n_1 = sorted(n,reverse=False)
n_2 = n_1.pop()

missing_num = (n_2*(n_2 +1)//2) - sum(n)