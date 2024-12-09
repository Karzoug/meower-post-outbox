package buildinfo

import (
	"runtime"
)

var bi = BuildInfo{
	BuildVersion:   buildVersion,
	BuildDate:      buildDate,
	ServiceVersion: serviceVersion,
	GoVersion:      runtime.Version(),
	OS:             runtime.GOOS,
	Arch:           runtime.GOARCH,
	Compiler:       runtime.Compiler,
}

// BuildInfo represents all available build information.
type BuildInfo struct {
	// Arch is an architecture of the machine used for the build
	Arch string
	// BuildDate is a date of the build
	BuildDate string
	// Version is a version of the build
	BuildVersion string
	// Compiler is a compiler used for the build
	Compiler string
	// GoVersion is a Go programming language version used for the build
	GoVersion string
	// Os is a operating system used for the build
	OS string
	// ServiceVersion is a version of the service (semver)
	ServiceVersion string
}

// Get returns all available build information.
func Get() BuildInfo {
	return bi
}
