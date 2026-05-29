def map_phrase(phrase: str) -> dict[str, int]:
    lowercase_phrase: str = phrase.lower()
    phrase_without_exclamation: str = lowercase_phrase.replace("!", " ")
    cleaned_phrase: str = phrase_without_exclamation.replace(".", " ")
    words: list[str] = cleaned_phrase.split()

    word_counts: dict[str, int] = {}

    for word in words:
        if len(word) > 2:
            if word in word_counts:
                word_counts[word] += 1
            else:
                word_counts[word] = 1

    return word_counts
