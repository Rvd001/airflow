x = 10
print(x)
# import keyword
# print(keyword.kwlist)

#Checking how None works

# a = 20
# print(a)
# b = print(a)
# print(b)

print(-5//2)
#This internal division performs floor by default
name = 'K'
#unicode representation
print(ord(name))

print(5+2.5)

x = input(' Type the first number ')
y = input(' Type the second number ')

#this outputs to an error because of difference in data type
# print('addition of both numbers equals:',int(x) + int(y))
print('addition of both numbers equals:',eval(x) + eval(y))