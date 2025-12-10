"""Векторизация текстов для Topic Modeling"""

from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer


def create_lda_vectorizer():
    """
    Создает векторизатор для LDA
    
    Returns:
        CountVectorizer для LDA
    """
    return CountVectorizer(
        max_features=1000,
        min_df=2,
        max_df=0.8,
        ngram_range=(1, 2)
    )


def create_nmf_vectorizer():
    """
    Создает векторизатор для NMF
    
    Returns:
        TfidfVectorizer для NMF
    """
    return TfidfVectorizer(
        max_features=1000,
        min_df=2,
        max_df=0.8,
        ngram_range=(1, 2)
    )

