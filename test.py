from tdqm import tdqm
k = [1,2,3,4,5,6]
pbar = tdqm([1,2,3,4,5,6])
print('poop')
for i in k:
    pbar.update()
print('done')