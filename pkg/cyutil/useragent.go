package cyutil

import (
	"regexp"
	"strings"
)

// ParseUserAgent extracts OS and browser information from a user agent string
// Returns a simplified string with OS type/version and browser type/version
func ParseUserAgent(ua string) string {
	if ua == "" {
		return "-"
	}

	// Extract OS information
	var os, osVersion string
	
	// Windows detection
	if strings.Contains(ua, "Windows") {
		os = "Windows"
		winVerRegex := regexp.MustCompile(`Windows NT (\d+\.\d+)`)
		if matches := winVerRegex.FindStringSubmatch(ua); len(matches) > 1 {
			switch matches[1] {
			case "10.0":
				osVersion = "10/11"
			case "6.3":
				osVersion = "8.1"
			case "6.2":
				osVersion = "8"
			case "6.1":
				osVersion = "7"
			case "6.0":
				osVersion = "Vista"
			case "5.2", "5.1":
				osVersion = "XP"
			default:
				osVersion = matches[1]
			}
		}
	} else if strings.Contains(ua, "Macintosh") || strings.Contains(ua, "Mac OS X") {
		os = "Mac"
		macVerRegex := regexp.MustCompile(`Mac OS X (\d+[._]\d+)`)
		if matches := macVerRegex.FindStringSubmatch(ua); len(matches) > 1 {
			osVersion = strings.Replace(matches[1], "_", ".", -1)
		}
	} else if strings.Contains(ua, "Linux") {
		os = "Linux"
		if strings.Contains(ua, "Android") {
			os = "Android"
			androidVerRegex := regexp.MustCompile(`Android (\d+\.\d+)`)
			if matches := androidVerRegex.FindStringSubmatch(ua); len(matches) > 1 {
				osVersion = matches[1]
			}
		}
	} else if strings.Contains(ua, "iPhone") || strings.Contains(ua, "iPad") || strings.Contains(ua, "iPod") {
		os = "iOS"
		iosVerRegex := regexp.MustCompile(`OS (\d+[._]\d+)`)
		if matches := iosVerRegex.FindStringSubmatch(ua); len(matches) > 1 {
			osVersion = strings.Replace(matches[1], "_", ".", -1)
		}
	}

	// Extract browser information
	var browser, browserVersion string
	
	// Chrome
	if strings.Contains(ua, "Chrome/") && !strings.Contains(ua, "Chromium") {
		browser = "Chrome"
		chromeVerRegex := regexp.MustCompile(`Chrome/(\d+\.\d+)`)
		if matches := chromeVerRegex.FindStringSubmatch(ua); len(matches) > 1 {
			browserVersion = matches[1]
		}
	} else if strings.Contains(ua, "Firefox/") {
		browser = "Firefox"
		ffVerRegex := regexp.MustCompile(`Firefox/(\d+\.\d+)`)
		if matches := ffVerRegex.FindStringSubmatch(ua); len(matches) > 1 {
			browserVersion = matches[1]
		}
	} else if strings.Contains(ua, "Safari/") && !strings.Contains(ua, "Chrome") && !strings.Contains(ua, "Chromium") {
		browser = "Safari"
		safariVerRegex := regexp.MustCompile(`Version/(\d+\.\d+)`)
		if matches := safariVerRegex.FindStringSubmatch(ua); len(matches) > 1 {
			browserVersion = matches[1]
		}
	} else if strings.Contains(ua, "MSIE ") || strings.Contains(ua, "Trident/") {
		browser = "IE"
		ieVerRegex := regexp.MustCompile(`MSIE (\d+\.\d+)`)
		if matches := ieVerRegex.FindStringSubmatch(ua); len(matches) > 1 {
			browserVersion = matches[1]
		} else {
			// IE 11 doesn't use "MSIE" in UA string
			browser = "IE"
			browserVersion = "11.0"
		}
	} else if strings.Contains(ua, "Edg/") {
		browser = "Edge"
		edgeVerRegex := regexp.MustCompile(`Edg/(\d+\.\d+)`)
		if matches := edgeVerRegex.FindStringSubmatch(ua); len(matches) > 1 {
			browserVersion = matches[1]
		}
	}

	// Format the result
	result := ""
	if os != "" {
		result += os
		if osVersion != "" {
			result += "/" + osVersion
		}
	}
	
	if browser != "" {
		if result != "" {
			result += " "
		}
		result += browser
		if browserVersion != "" {
			result += "/" + browserVersion
		}
	}
	
	if result == "" {
		return "Unknown"
	}
	
	return result
}
