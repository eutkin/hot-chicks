package com.github.eutkin

import io.micronaut.runtime.Micronaut.*

fun main(args: Array<String>) {
    build()
        .args(*args)
        .packages("com.github.eutkin")
        .eagerInitSingletons(true)
        .start()
}

