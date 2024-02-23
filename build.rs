// Copyright (c) 2024, Green Man Gaming Limited
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fs;
use walkdir::WalkDir;

/// This build script is used to check that all the views are valid TOML and that the aggregation
/// pipeline is valid JSON. It will re-run if any of the files in the views directory change.
fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=views/");

    let walker = WalkDir::new("./views").into_iter();

    for entry in walker {
        let entry = match entry {
            Ok(entry) => entry,
            Err(_) => continue,
        };

        let path = entry.path();

        let file_name = match path.file_name() {
            Some(file_name) => file_name,
            None => continue,
        };

        let file_name_str = match file_name.to_str() {
            Some(s) => s,
            None => continue,
        };

        if !file_name_str.ends_with(".toml") {
            continue;
        }

        let contents = match fs::read_to_string(path) {
            Ok(c) => c,
            Err(_) => {
                panic!("could not read file");
            }
        };

        let json_values = toml::from_str::<serde_json::Value>(&contents).unwrap();
        let aggregates = json_values.get("aggregation").unwrap().as_array().unwrap();
        let reduces = json_values.get("reduce").and_then(|v| v.as_object());

        eprintln!("file: {}", entry.path().display());

        // Get the string from each aggregates entry and then try and parse as json, panicking if
        // not
        let _aggregates: Vec<serde_json::Value> = aggregates
            .iter()
            .map(|v| serde_json::from_str(v.as_str().unwrap()).unwrap())
            .collect();

        if let Some(r) = reduces {
            r.iter().for_each(|(_k, v)| {
                let a = v.get("aggregation").unwrap().as_array().unwrap();
                let _aggregates: Vec<serde_json::Value> = a
                    .iter()
                    .map(|v| serde_json::from_str(v.as_str().unwrap()).unwrap())
                    .collect();
            });
        }
    }
}
