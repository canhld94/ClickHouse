#pragma once

#include <Access/IAccessEntity.h>
#include <Access/AccessRights.h>
#include <Access/GrantedRoles.h>
#include <Access/SettingsProfileElement.h>


namespace DB
{

struct Role : public IAccessEntity
{
    AccessRights access;
    GrantedRoles granted_roles;
    SettingsProfileElements settings;

    bool equal(const IAccessEntity & other) const override;
    std::shared_ptr<IAccessEntity> clone() const override { return cloneImpl<Role>(); }
    static constexpr const auto TYPE = AccessEntityType::ROLE;
    AccessEntityType getType() const override { return TYPE; }

    std::vector<UUID> findDependencies() const override;
    bool hasDependencies(const std::unordered_set<UUID> & ids) const override;
    void replaceDependencies(const std::unordered_map<UUID, UUID> & old_to_new_ids) override;
    void copyDependenciesFrom(const IAccessEntity & src, const std::unordered_set<UUID> & ids) override;
    void removeDependencies(const std::unordered_set<UUID> & ids) override;
    void clearAllExceptDependencies() override;

    bool isBackupAllowed() const override { return settings.isBackupAllowed(); }
};

using RolePtr = std::shared_ptr<const Role>;
}
