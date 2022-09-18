package com.example.esfiles

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class EsPdfApplication

fun main(args: Array<String>) {
    runApplication<EsPdfApplication>(*args)
}
