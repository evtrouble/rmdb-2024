#pragma once

#include <cstdint>
#include <vector>
#include <memory>
#include <optional>
#include <queue>
#include <string>
#include <utility>
#include "defs.h"

enum class IteratorType {
  SkipListIterator,
  MemTableIterator,
  SstIterator,
  BlockIterator,
  HeapIterator,
  TwoMergeIterator,
  ConcactIterator,
  LevelIterator,
};

class BaseIterator {
public:
  using key_type = std::string;
  using value_type = Rid;
  using pointer = value_type *;
  using reference = value_type &;
  using T = std::pair<key_type, value_type>;

  virtual BaseIterator &operator++() = 0;
  virtual bool operator==(const BaseIterator &other) const = 0;
  virtual bool operator!=(const BaseIterator &other) const = 0;
  virtual T& operator*() const = 0;
  virtual T* operator->() const = 0;
  virtual IteratorType get_type() const = 0;
  virtual bool is_end() const = 0;
};

class MergeIterator : public BaseIterator
{
  //多路归并
  std::vector<std::shared_ptr<BaseIterator>> iters;

public:
  virtual BaseIterator &operator++() override;
  virtual bool operator==(const BaseIterator &other) const override;
  virtual bool operator!=(const BaseIterator &other) const override;
  virtual T& operator*() const override;
  virtual T* operator->() const override;
  virtual IteratorType get_type() const override;
  virtual bool is_end() const override;
};