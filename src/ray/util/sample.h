#ifndef RAY_UTIL_SAMPLE_H
#define RAY_UTIL_SAMPLE_H

#include "absl/random/random.h"
#include "absl/random/uniform_int_distribution.h"

// Randomly samples num_elements from the elements between first and last using reservoir
// sampling.
template <class Iterator, class T = typename std::iterator_traits<Iterator>::value_type>
std::vector<T> random_sample(Iterator begin, Iterator end, size_t num_elements) {
  absl::BitGen gen;
  std::vector<T> result;
  if (num_elements == 0) {
    return result;
  }

  size_t current_index = 0;
  for (auto it = begin; it != end; it++) {
    if (current_index < num_elements) {
      result.push_back(*it);
    } else {
      size_t random_index = absl::uniform_int_distribution<size_t>(0, current_index)(gen);
      if (random_index < num_elements) {
        result.at(random_index) = *it;
      }
    }
    current_index++;
  }
  return result;
}

#endif  // RAY_UTIL_SAMPLE_H
