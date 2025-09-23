package cydb

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/fj1981/infrakit/pkg/cylog"
	"github.com/fj1981/infrakit/pkg/cyutil"
)

func (d *DBCli) migrateFromFolder(pm *MigrateSQLParam, migrationsPath string) error {
	migrateTableName := "_migrations"
	if pm.serviceOwner != "" {
		serviceOwner := strings.ToLower(pm.serviceOwner)
		serviceOwner = strings.ReplaceAll(serviceOwner, "-", "_")
		migrateTableName = fmt.Sprintf("%s_migrations", serviceOwner)
	} else {
		migrateTableName = "gorp_migrations"
	}
	if ok, err := d.IsTableExist(migrateTableName); err != nil {
		return err
	} else if !ok {
		if sqlFunc, ok := GetSqlDialect(d.dbtype); ok {
			createSQL := fmt.Sprintf(`
			CREATE TABLE %s (id %s not null primary key, applied_at %s);
			`, migrateTableName, sqlFunc.GetDefaultTypeName(DefaultDBFieldTypeString), sqlFunc.GetDefaultTypeName(DefaultDBFieldTypeTime))
			_, err := d.excute(createSQL)
			if err != nil {
				if pm.ignoreError {
					slog.Error(fmt.Sprintf("create table %s failed: %s", migrateTableName, err.Error()))
					return nil
				}
				return err
			}
		} else {
			if pm.ignoreError {
				slog.Error("not support db type: " + d.dbtype)
				return nil
			}
			return errors.New("not support db type: " + d.dbtype)
		}
	}
	rows, err := d.List(migrateTableName, nil)
	if err != nil {
		if pm.ignoreError {
			slog.Error("list table " + migrateTableName + " failed: " + err.Error())
			return nil
		}
		return err
	}
	filesSet := make(map[string]struct{})
	for _, row := range rows {
		filesSet[cyutil.GetStr(row, "id", true)] = struct{}{}
	}
	fileList := make([]string, 0)
	err = fs.WalkDir(pm.fs, migrationsPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			if strings.HasSuffix(path, ".sql") {
				fileList = append(fileList, path)
			}
		}
		return nil
	})
	if err != nil {
		if pm.ignoreError {
			slog.Error("walk dir " + migrationsPath + " failed: " + err.Error())
			return nil
		}
		return err
	}
	sort.Strings(fileList)
	pathNeedMerge := make([]string, 0)
	for _, file := range fileList {
		base := filepath.Base(file)
		if _, ok := filesSet[base]; ok {
			delete(filesSet, base)
			continue
		} else {
			pathNeedMerge = append(pathNeedMerge, file)
		}
	}
	if len(filesSet) > 0 {
		if pm.ignoreError {
			slog.Error("some files not found in " + migrateTableName)
			return nil
		}
		return errors.New("some files not found in " + migrateTableName)
	}
	funcExcuteSql := func(block *SQLStatement) error {
		if block == nil || len(strings.TrimSpace(block.Content)) == 0 {
			return nil
		}
		_, err := d.Excute(block.Content)
		if err != nil {
			return err
		}
		return nil
	}

	funcExcuteFile := func(sqlfilePath string, mergeFlag string) error {
		f, err := pm.fs.Open(sqlfilePath)
		if err != nil {
			return err
		}
		defer f.Close()
		if err := d.ReadSQLFile(f, funcExcuteSql, WithMigrationMode(mergeFlag)); err != nil {
			return err
		}
		return nil
	}

	errFileList := make([]string, 0)
	for _, sqlfilePath := range pathNeedMerge {
		if err := funcExcuteFile(sqlfilePath, "Up"); err != nil {
			errFileList = append(errFileList, sqlfilePath)
			return err
		}
		if _, err := d.Insert(migrateTableName, map[string]any{
			"id":         filepath.Base(sqlfilePath),
			"applied_at": time.Now(),
		}); err != nil {
			errFileList = append(errFileList, sqlfilePath)
			return err
		}
	}
	if len(errFileList) > 0 {
		for _, file := range errFileList {
			funcExcuteFile(file, "Down")
		}
		if pm.ignoreError {
			cylog.Skip(0).Error("some files failed to execute: " + strings.Join(errFileList, ","))
			return nil
		}
		return errors.New("some files failed to execute: " + strings.Join(errFileList, ","))
	} else {
		cylog.Skip(0).Infof("migrate(%d) success", len(pathNeedMerge))
	}
	return nil
}
