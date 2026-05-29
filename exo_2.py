def map_phrase(phrase: str) -> list[tuple[str, int]]:
    lowercase_phrase: str = phrase.lower()
    phrase_without_exclamation: str = lowercase_phrase.replace("!", " ")
    cleaned_phrase: str = phrase_without_exclamation.replace(".", " ")
    words: list[str] = cleaned_phrase.split()

    mapped_words: list[tuple[str, int]] = []

    for word in words:
        if len(word) > 2:
            mapped_words.append((word, 1))

    return mapped_words


def shuffle_and_sort(mapped_words: list[tuple[str, int]]) -> dict[str, list[int]]:
    grouped_words: dict[str, list[int]] = {}

    for word, count in mapped_words:
        if word in grouped_words:
            grouped_words[word].append(count)
        else:
            grouped_words[word] = [count]

    return grouped_words


def reduce(grouped_words: dict[str, list[int]]) -> dict[str, int]:
    reduced_words: dict[str, int] = {}

    for word, counts in grouped_words.items():
        reduced_words[word] = sum(counts)

    return reduced_words