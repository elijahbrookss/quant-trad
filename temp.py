import string


def find_unique_char (s: str):
	letter_to_number = {}

	count = 1
	for letter in s:
		if letter_to_number.get(letter):
			count += 1
			letter_to_number[letter] = count
		else:
			letter_to_number[letter] = count

	print(letter_to_number)
	for index, letter in enumerate(letter_to_number):
		if letter_to_number[letter] == 1:
			return index

	return -1
  
print(find_unique_char("leetcode"))
print(find_unique_char("aabbcc"))
print(find_unique_char("orange"))