package ru.bdsnowflake.lr3;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public record CustomerRecord(
        String customerKey,
        Long customerSourceId,
        String firstName,
        String lastName,
        Integer age,
        String email,
        String country,
        String postalCode,
        String petType,
        String petName,
        String petBreed,
        String petCategory
) {
    public static CustomerRecord fromEvent(RawSaleEvent event) {
        return new CustomerRecord(
                PetShopStreamingJob.stableKey(
                        "customer",
                        event.customer_first_name,
                        event.customer_last_name,
                        event.customer_email,
                        event.customer_country
                ),
                PetShopStreamingJob.longValue(event.sale_customer_id),
                event.customer_first_name,
                event.customer_last_name,
                PetShopStreamingJob.integer(event.customer_age),
                event.customer_email,
                event.customer_country,
                event.customer_postal_code,
                event.customer_pet_type,
                event.customer_pet_name,
                event.customer_pet_breed,
                event.pet_category
        );
    }

    public static void bind(PreparedStatement statement, CustomerRecord record) throws SQLException {
        statement.setString(1, record.customerKey);
        if (record.customerSourceId == null) {
            statement.setNull(2, java.sql.Types.BIGINT);
        } else {
            statement.setLong(2, record.customerSourceId);
        }
        statement.setString(3, record.firstName);
        statement.setString(4, record.lastName);
        if (record.age == null) {
            statement.setNull(5, java.sql.Types.INTEGER);
        } else {
            statement.setInt(5, record.age);
        }
        statement.setString(6, record.email);
        statement.setString(7, record.country);
        statement.setString(8, record.postalCode);
        statement.setString(9, record.petType);
        statement.setString(10, record.petName);
        statement.setString(11, record.petBreed);
        statement.setString(12, record.petCategory);
    }
}
