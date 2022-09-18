package com.example.esfiles.filestorage

import java.nio.file.Path;
import java.util.stream.Stream;
 
import org.springframework.core.io.Resource;
import org.springframework.web.multipart.MultipartFile;

interface FileStorage {
	fun store(file: MultipartFile)
	fun loadFile(filename: String): Resource
	fun deleteAll()
	fun init()
	fun loadFiles(): Stream<Path>
}