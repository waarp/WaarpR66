package org.waarp.openr66.protocol.http.restv2.handler;

import co.cask.http.AbstractHttpHandler;
import io.netty.handler.codec.http.HttpMethod;
import org.waarp.openr66.dao.BusinessDAO;
import org.waarp.openr66.dao.exception.DAOException;
import org.waarp.openr66.protocol.http.restv2.RestUtils;
import org.waarp.openr66.protocol.http.restv2.data.RestHostConfig;

import java.util.Arrays;
import java.util.List;

public abstract class AbstractRestHttpHandler extends AbstractHttpHandler {

    private final List<RestHostConfig.RoleType> writeRoles;

    private final List<RestHostConfig.RoleType> readRoles;

    protected AbstractRestHttpHandler(List<RestHostConfig.RoleType> writeRoles, List<RestHostConfig.RoleType> readRoles) {
        this.writeRoles = writeRoles;
        this.readRoles = readRoles;
    }


    public boolean isAuthorized(String user, HttpMethod httpMethod, String method) throws DAOException {
        BusinessDAO businessDAO = RestUtils.factory.getBusinessDAO();
        RestHostConfig.Role[] roles = RestHostConfig.Role.toRoleList(businessDAO.select(RestUtils.HOST_ID).getRoles());
        RestHostConfig.RoleType[] rights = null;
        for (RestHostConfig.Role role : roles) {
            if (role.host.equals(user)) {
                rights = role.roleTypes;
            }
        }

        if (httpMethod == HttpMethod.OPTIONS) {
            return true;
        } else if (rights == null || Arrays.asList(rights).contains(RestHostConfig.RoleType.noAccess)) {
            return false;
        } else {
            for (RestHostConfig.RoleType right : rights) {
                if ((httpMethod == HttpMethod.GET && readRoles.contains(right)) || writeRoles.contains(right)) {
                    return true;
                }
            }
            return false;
        }
    }
}
