"""
Фильтрация брендов
"""

import sys
import importlib.util
from pathlib import Path
from typing import Set, Dict, List


class BrandFilter:
    """Фильтр брендов"""
    
    @staticmethod
    def filter_brands(
        word_counter: Dict[str, int],
        capitalized_counter: Dict[str, int],
        context_counter: Dict[str, Set[str]],
        word_forms: Dict[str, Set[str]],
        topic_words: Set[str],
        min_occurrences: int = 3,
        capitalization_threshold: float = 0.5,
        filter_common_words: bool = True,
        common_words_vocab: Set[str] = None,
        morph_vocab=None,
        cap_fixer=None
    ) -> Set[str]:
        """
        Фильтрует бренды из слов
        
        Args:
            word_counter: Счетчик слов
            capitalized_counter: Счетчик капитализированных слов
            context_counter: Контексты слов
            word_forms: Формы написания слов
            topic_words: Слова-темы для исключения
            min_occurrences: Минимум встреч
            capitalization_threshold: Порог капитализации
            filter_common_words: Фильтровать ли обычные слова
            common_words_vocab: Словарь частотных слов
            morph_vocab: Морфологический словарь
            cap_fixer: Фиксатор капитализации
            
        Returns:
            Множество найденных брендов
        """
        backup_path = Path(__file__).parent.parent / 'brand_learner.py.backup'
        if backup_path.exists():
            spec = importlib.util.spec_from_file_location("brand_learner_backup", backup_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            temp_instance = module.BrandLearner(
                min_occurrences=min_occurrences,
                capitalization_threshold=capitalization_threshold,
                filter_common_words=filter_common_words
            )
            temp_instance.common_words_vocab = common_words_vocab or set()
            temp_instance.morph_vocab = morph_vocab
            temp_instance.cap_fixer = cap_fixer
            
            # Используем внутреннюю логику
            learned_brands = set()
            for word_lower, total_count in word_counter.items():
                if word_lower in topic_words:
                    continue
                
                is_latin = temp_instance._is_latin(word_lower)
                is_acronym = temp_instance._is_likely_acronym_by_structure(word_lower)
                has_diverse_contexts = len(context_counter.get(word_lower, set())) >= 3
                is_potential_brand = temp_instance._looks_like_brand_name(word_lower, has_diverse_contexts, total_count)
                
                if is_latin and len(word_lower) >= 3 and not word_lower.isdigit():
                    best_form = temp_instance._generate_brand_form(word_lower, word_forms.get(word_lower, {word_lower}))
                    learned_brands.add(best_form)
                elif is_acronym and total_count >= min_occurrences:
                    if not is_latin:
                        if common_words_vocab and word_lower in common_words_vocab:
                            continue
                        if morph_vocab:
                            parsed = morph_vocab.parse(word_lower)
                            if parsed:
                                lemma = parsed[0].normal_form
                                if common_words_vocab and lemma in common_words_vocab:
                                    continue
                                if lemma in topic_words:
                                    continue
                        num_forms = len(word_forms.get(word_lower, set()))
                        if num_forms > 3 or total_count < 5:
                            continue
                    best_form = temp_instance._generate_brand_form(word_lower, word_forms.get(word_lower, {word_lower}))
                    learned_brands.add(best_form)
                elif has_diverse_contexts and is_potential_brand and total_count >= 5:
                    cap_count = capitalized_counter.get(word_lower, 0)
                    if not is_latin:
                        if cap_count == 0:
                            continue
                        if common_words_vocab and word_lower in common_words_vocab:
                            continue
                        if morph_vocab:
                            parsed = morph_vocab.parse(word_lower)
                            if parsed:
                                lemma = parsed[0].normal_form
                                if lemma in topic_words or (common_words_vocab and lemma in common_words_vocab):
                                    continue
                        num_forms = len(word_forms.get(word_lower, set()))
                        if num_forms > 4:
                            continue
                    best_form = temp_instance._generate_brand_form(word_lower, word_forms.get(word_lower, {word_lower}))
                    learned_brands.add(best_form)
            
            return learned_brands
        else:
            return set()

