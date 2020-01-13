class StringExercise:

    def __init__(self):
        pass   # Do some initial setup in this constructor method, if needed

    def reverse_string(self, input_str):
        """
        Reverses order of characters in string input_str.
        """
        return input_str[::-1]

    def is_english_vowel(self, character):
        """
        Returns True if character is an english vowel
        and False otherwise.
        """
        result = False
        vowels_list = ['a','e','i','o','u']
        if character.lower() in vowels_list:
            result = True
        return result
            

    def find_longest_word(self, sentence):
        """
        Returns the longest word in string sentence.
        In case there are several, return the first.
        """
        longest_word=""
        sentence_list = sentence.split(' ')
        for word in sentence_list:
            if(len(longest_word)) <  len(str(word)):
                longest_word = str(word)
        return longest_word

    def get_word_lengths(self, text):
        """
        Returns a list of integers representing
        the word lengths in string text.
        """
        result_list = []
        for word in text.split(' '):
            result_list.append(len(word))
        return result_list