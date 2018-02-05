
# Chucky

Welcome to the chucky project. The goal of the project is to make firmware
updates of small devices faster by the idea that the update will probably
contain mostly the same content as the previous version. The idea for this
was taken from the casync program. casync might not be fit for microcontrollers
where chucky is intended for microcontrollers!

# Structure

- chucky is the program that slices the firmware image in seperate chunks
- tiffany is an implementation of the update algorithm in C which can be run
  on a microcontroller.

# Build status

[![Build Status](https://travis-ci.org/windelbouwman/chucky.svg?branch=master)](https://travis-ci.org/windelbouwman/chucky)


# Ideas

- hash calculation on target is not required, since we keep the table with
  old hashes
- TBD: use leb128 to store integers to save space?

